#include "pipestream/transport.hpp"

#include "pipestream/wire.hpp"

#include <msquic.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace pipestream {
namespace {

constexpr QUIC_REGISTRATION_CONFIG kRegistration{
    "pipestream-msquic", QUIC_EXECUTION_PROFILE_LOW_LATENCY};

const QUIC_BUFFER kAlpnBuffer{
    static_cast<std::uint32_t>(pipestream::kAlpn.size()),
    reinterpret_cast<std::uint8_t*>(const_cast<char*>(pipestream::kAlpn.data()))};

std::runtime_error quic_error(std::string_view operation, QUIC_STATUS status) {
  std::ostringstream message;
  message << operation << " failed with MsQuic status 0x" << std::hex
          << static_cast<std::uint32_t>(status);
  return std::runtime_error(message.str());
}

void check(QUIC_STATUS status, std::string_view operation) {
  if (QUIC_FAILED(status)) throw quic_error(operation, status);
}

std::vector<std::uint8_t> read_file(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("cannot read " + path.string());
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void write_file(const std::filesystem::path& path, const std::vector<std::uint8_t>& data) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output) throw std::runtime_error("cannot write " + path.string());
  output.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
  if (!output) throw std::runtime_error("failed writing " + path.string());
}

class Completion {
 public:
  void succeed() {
    std::lock_guard lock(mutex_);
    if (done_) return;
    done_ = true;
    condition_.notify_all();
  }

  void fail(std::string message) {
    std::lock_guard lock(mutex_);
    if (done_) return;
    error_ = std::move(message);
    done_ = true;
    condition_.notify_all();
  }

  void wait(std::chrono::seconds timeout) {
    std::unique_lock lock(mutex_);
    if (!condition_.wait_for(lock, timeout, [this] { return done_; })) {
      throw std::runtime_error("timed out waiting for QUIC session completion");
    }
    if (error_) throw std::runtime_error(*error_);
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  bool done_{false};
  std::optional<std::string> error_;
};

class Runtime {
 public:
  Runtime() {
    check(MsQuicOpen2(&api_), "MsQuicOpen2");
    try {
      check(api_->RegistrationOpen(&kRegistration, &registration_), "RegistrationOpen");
    } catch (...) {
      MsQuicClose(api_);
      api_ = nullptr;
      throw;
    }
  }

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  ~Runtime() {
    if (registration_ != nullptr) api_->RegistrationClose(registration_);
    if (api_ != nullptr) MsQuicClose(api_);
  }

  [[nodiscard]] const QUIC_API_TABLE* api() const { return api_; }
  [[nodiscard]] HQUIC registration() const { return registration_; }

 private:
  const QUIC_API_TABLE* api_{nullptr};
  HQUIC registration_{nullptr};
};

class Configuration {
 public:
  Configuration(const Runtime& runtime, const QUIC_SETTINGS& settings)
      : api_(runtime.api()) {
    check(api_->ConfigurationOpen(
              runtime.registration(), &kAlpnBuffer, 1, &settings, sizeof(settings), nullptr, &handle_),
          "ConfigurationOpen");
  }

  Configuration(const Configuration&) = delete;
  Configuration& operator=(const Configuration&) = delete;

  ~Configuration() {
    if (handle_ != nullptr) api_->ConfigurationClose(handle_);
  }

  [[nodiscard]] HQUIC handle() const { return handle_; }

 private:
  const QUIC_API_TABLE* api_;
  HQUIC handle_{nullptr};
};

struct SendContext {
  explicit SendContext(std::vector<std::uint8_t> input) : bytes(std::move(input)) {
    buffer.Length = static_cast<std::uint32_t>(bytes.size());
    buffer.Buffer = bytes.data();
  }

  std::vector<std::uint8_t> bytes;
  QUIC_BUFFER buffer{};
};

void send_bytes(
    const QUIC_API_TABLE* api,
    HQUIC stream,
    std::vector<std::uint8_t> bytes,
    QUIC_SEND_FLAGS flags = QUIC_SEND_FLAG_NONE) {
  auto context = std::make_unique<SendContext>(std::move(bytes));
  const QUIC_STATUS status = api->StreamSend(stream, &context->buffer, 1, flags, context.get());
  if (QUIC_FAILED(status)) throw quic_error("StreamSend", status);
  (void)context.release();
}

std::uint32_t network_u32(const std::uint8_t* input) {
  return (static_cast<std::uint32_t>(input[0]) << 24) |
         (static_cast<std::uint32_t>(input[1]) << 16) |
         (static_cast<std::uint32_t>(input[2]) << 8) |
         static_cast<std::uint32_t>(input[3]);
}

struct ServerSession;
struct ClientSession;

struct ServerStream {
  std::shared_ptr<ServerSession> session;
  bool control;
  std::vector<std::uint8_t> input;
};

struct ServerConnection {
  std::shared_ptr<ServerSession> session;
};

struct ServerSession : std::enable_shared_from_this<ServerSession> {
  ServerSession(
      const QUIC_API_TABLE* api_value,
      HQUIC connection_value,
      std::filesystem::path output_value,
      Completion* completion_value)
      : api(api_value),
        connection(connection_value),
        output_directory(std::move(output_value)),
        completion(completion_value) {}

  const QUIC_API_TABLE* api;
  HQUIC connection;
  std::filesystem::path output_directory;
  Completion* completion;
  std::mutex mutex;
  HQUIC control{nullptr};
  bool capabilities{false};
  std::optional<std::uint32_t> pending;
  std::optional<Entity> entity;
  bool entity_complete{false};
  bool cursor{false};
  std::atomic_bool protocol_complete{false};
  std::atomic_bool failed{false};

  void fail(const std::exception& error) {
    std::lock_guard lock(mutex);
    fail_locked(error.what(), dynamic_cast<const ProtocolError*>(&error));
  }

  void fail_locked(std::string message, const ProtocolError* protocol = nullptr) {
    if (failed.load()) return;
    failed.store(true);
    completion->fail(message);
    api->ConnectionShutdown(
        connection,
        QUIC_CONNECTION_SHUTDOWN_FLAG_NONE,
        protocol == nullptr ? kErrorFrame : protocol->code());
  }

  void control_frame(const ControlFrame& frame) {
    std::lock_guard lock(mutex);
    try {
      if (!capabilities) {
        if (frame.type != kFrameCapabilities) {
          throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", "first frame must be CAPABILITIES");
        }
        const auto negotiated = Capabilities{}.negotiate(decode_capabilities(frame.payload));
        send_bytes(api, control, encode_capabilities(negotiated));
        send_bytes(
            api,
            control,
            encode_status(Status{kStatusUnspecified, kConnectionLevel, 0, std::nullopt, 0}));
        capabilities = true;
        return;
      }
      if (frame.type == kFrameStatus) {
        const Status status = decode_status(frame.payload);
        if (status.state == kStatusUnspecified && status.entity_id == kConnectionLevel &&
            !status.cursor) {
          return;
        }
        if (!pending) {
          if (status.state != kStatusPending) {
            throw ProtocolError(kErrorEntityInvalid, "PIPESTREAM_ENTITY_INVALID", "first entity status must be PENDING");
          }
          pending = status.entity_id;
          maybe_process_locked();
        } else {
          if (status.state != kStatusUnspecified || status.entity_id != kConnectionLevel || !status.cursor ||
              *status.cursor != next_entity_id(*pending)) {
            throw ProtocolError(kErrorEntityInvalid, "PIPESTREAM_ENTITY_INVALID", "invalid connection-level cursor update");
          }
          cursor = true;
        }
        return;
      }
      if (frame.type == kFrameCheckpoint) {
        Checkpoint checkpoint = decode_checkpoint(frame.payload);
        if (!entity_complete || !pending || checkpoint.flags != 0 ||
            checkpoint.checkpoint_entity_id != next_entity_id(*pending)) {
          throw ProtocolError(
              kErrorEntityInvalid,
              "PIPESTREAM_ENTITY_INVALID",
              "checkpoint barrier is not satisfied");
        }
        checkpoint.flags = kCheckpointAck;
        send_bytes(api, control, encode_checkpoint(checkpoint));
        return;
      }
      if (frame.type == kFrameGoaway) {
        if (!cursor || !pending || decode_goaway(frame.payload) != *pending) {
          throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", "invalid GOAWAY");
        }
        send_bytes(api, control, encode_goaway(*pending), QUIC_SEND_FLAG_FIN);
        protocol_complete.store(true);
        return;
      }
      throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", "unexpected control frame type");
    } catch (const std::exception& error) {
      fail_locked(error.what(), dynamic_cast<const ProtocolError*>(&error));
    }
  }

  void received_entity(std::vector<std::uint8_t> bytes) {
    std::lock_guard lock(mutex);
    try {
      entity = decode_entity(bytes);
      maybe_process_locked();
    } catch (const std::exception& error) {
      fail_locked(error.what(), dynamic_cast<const ProtocolError*>(&error));
    }
  }

  void maybe_process_locked() {
    if (!pending || !entity) return;
    if (entity->header.entity_id != *pending) {
      throw ProtocolError(kErrorEntityInvalid, "PIPESTREAM_ENTITY_INVALID", "PENDING and EntityHeader IDs differ");
    }
    send_bytes(
        api,
        control,
        encode_status(Status{kStatusProcessing, *pending, 0, std::nullopt, 0}));
    write_file(output_directory / (std::to_string(*pending) + ".bin"), entity->payload);
    if (entity->header.parent_id) {
      const std::string parent = std::to_string(*entity->header.parent_id) + "\n";
      write_file(
          output_directory / (std::to_string(*pending) + ".parent"),
          std::vector<std::uint8_t>(parent.begin(), parent.end()));
    }
    send_bytes(
        api,
        control,
        encode_status(Status{kStatusComplete, *pending, 0, std::nullopt, 0}));
    entity_complete = true;
    std::cout << "RECEIVED " << *pending << ' ' << entity->payload.size() << '\n' << std::flush;
  }
};

QUIC_STATUS QUIC_API server_stream_callback(HQUIC stream, void* context, QUIC_STREAM_EVENT* event) {
  auto* state = static_cast<ServerStream*>(context);
  try {
    switch (event->Type) {
      case QUIC_STREAM_EVENT_RECEIVE:
        for (std::uint32_t index = 0; index < event->RECEIVE.BufferCount; ++index) {
          const QUIC_BUFFER& buffer = event->RECEIVE.Buffers[index];
          if (state->input.size() + buffer.Length > kMaxPayload + kMaxEntityHeader + 4) {
            throw ProtocolError(kErrorLimitExceeded, "PIPESTREAM_LIMIT_EXCEEDED", "stream exceeds local limit");
          }
          state->input.insert(state->input.end(), buffer.Buffer, buffer.Buffer + buffer.Length);
        }
        if (state->control) {
          while (state->input.size() >= 5) {
            const std::size_t length = network_u32(state->input.data() + 1);
            if (length > kMaxControlFrame) {
              throw ProtocolError(kErrorLimitExceeded, "PIPESTREAM_LIMIT_EXCEEDED", "control frame exceeds local limit");
            }
            if (state->input.size() < length + 5) break;
            std::vector<std::uint8_t> frame_bytes(state->input.begin(), state->input.begin() + static_cast<std::ptrdiff_t>(length + 5));
            state->input.erase(state->input.begin(), state->input.begin() + static_cast<std::ptrdiff_t>(length + 5));
            state->session->control_frame(decode_control(frame_bytes));
          }
        }
        break;
      case QUIC_STREAM_EVENT_SEND_COMPLETE:
        delete static_cast<SendContext*>(event->SEND_COMPLETE.ClientContext);
        break;
      case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        if (!state->control) state->session->received_entity(std::move(state->input));
        break;
      case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        throw ProtocolError(kErrorFrame, "PIPESTREAM_CONTROL_RESET", "peer aborted stream");
      case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        if (!event->SHUTDOWN_COMPLETE.AppCloseInProgress) state->session->api->StreamClose(stream);
        delete state;
        break;
      default:
        break;
    }
  } catch (const std::exception& error) {
    state->session->fail(error);
  }
  return QUIC_STATUS_SUCCESS;
}

QUIC_STATUS QUIC_API server_connection_callback(HQUIC connection, void* context, QUIC_CONNECTION_EVENT* event) {
  auto* state = static_cast<ServerConnection*>(context);
  try {
    switch (event->Type) {
      case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
        const bool unidirectional =
            (event->PEER_STREAM_STARTED.Flags & QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL) != 0;
        if (!unidirectional && state->session->control != nullptr) {
          throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", "more than one bidirectional stream");
        }
        auto* stream_state = new ServerStream{state->session, !unidirectional, {}};
        if (!unidirectional) state->session->control = event->PEER_STREAM_STARTED.Stream;
        state->session->api->SetCallbackHandler(
            event->PEER_STREAM_STARTED.Stream,
            reinterpret_cast<void*>(server_stream_callback),
            stream_state);
        break;
      }
      case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        if (!state->session->protocol_complete.load()) {
          state->session->completion->fail("MsQuic transport closed before protocol completion");
        }
        break;
      case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        if (event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode != kErrorNoError) {
          state->session->completion->fail("peer closed connection with an application error");
        }
        break;
      case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        if (state->session->protocol_complete.load() && !state->session->failed.load()) {
          state->session->completion->succeed();
        } else if (!state->session->failed.load()) {
          state->session->completion->fail("connection ended before Layer 0 completion");
        }
        if (!event->SHUTDOWN_COMPLETE.AppCloseInProgress) state->session->api->ConnectionClose(connection);
        delete state;
        break;
      default:
        break;
    }
  } catch (const std::exception& error) {
    state->session->fail(error);
  }
  return QUIC_STATUS_SUCCESS;
}

struct ServerRunner {
  const QUIC_API_TABLE* api;
  HQUIC configuration;
  std::filesystem::path output_directory;
  Completion completion;
};

QUIC_STATUS QUIC_API server_listener_callback(HQUIC, void* context, QUIC_LISTENER_EVENT* event) {
  auto* server = static_cast<ServerRunner*>(context);
  if (event->Type != QUIC_LISTENER_EVENT_NEW_CONNECTION) return QUIC_STATUS_NOT_SUPPORTED;
  try {
    auto session = std::make_shared<ServerSession>(
        server->api,
        event->NEW_CONNECTION.Connection,
        server->output_directory,
        &server->completion);
    auto* connection_state = new ServerConnection{std::move(session)};
    server->api->SetCallbackHandler(
        event->NEW_CONNECTION.Connection,
        reinterpret_cast<void*>(server_connection_callback),
        connection_state);
    const QUIC_STATUS status = server->api->ConnectionSetConfiguration(
        event->NEW_CONNECTION.Connection, server->configuration);
    if (QUIC_FAILED(status)) {
      delete connection_state;
      return status;
    }
    return QUIC_STATUS_SUCCESS;
  } catch (const std::exception& error) {
    server->completion.fail(error.what());
    return QUIC_STATUS_OUT_OF_MEMORY;
  }
}

struct ClientStream {
  std::shared_ptr<ClientSession> session;
  bool control;
  std::vector<std::uint8_t> input;
};

QUIC_STATUS QUIC_API client_stream_callback(
    HQUIC stream, void* context, QUIC_STREAM_EVENT* event);

struct ClientConnection {
  std::shared_ptr<ClientSession> session;
};

struct ClientSession : std::enable_shared_from_this<ClientSession> {
  ClientSession(
      const QUIC_API_TABLE* api_value,
      Completion* completion_value,
      std::uint32_t entity_id_value,
      std::vector<std::uint8_t> payload_value,
      std::string content_type_value,
      std::optional<std::uint32_t> parent_id_value)
      : api(api_value),
        completion(completion_value),
        entity_id(entity_id_value),
        payload(std::move(payload_value)),
        content_type(std::move(content_type_value)),
        parent_id(parent_id_value) {}

  const QUIC_API_TABLE* api;
  HQUIC connection;
  Completion* completion;
  std::uint32_t entity_id;
  std::vector<std::uint8_t> payload;
  std::string content_type;
  std::optional<std::uint32_t> parent_id;
  std::mutex mutex;
  HQUIC control{nullptr};
  bool capabilities{false};
  bool processing{false};
  bool complete{false};
  bool checkpoint_acknowledged{false};
  std::atomic_bool protocol_complete{false};
  std::atomic_bool failed{false};

  void fail(const std::exception& error) {
    std::lock_guard lock(mutex);
    fail_locked(error.what(), dynamic_cast<const ProtocolError*>(&error));
  }

  void fail_locked(std::string message, const ProtocolError* protocol = nullptr) {
    if (failed.load()) return;
    failed.store(true);
    completion->fail(message);
    api->ConnectionShutdown(
        connection,
        QUIC_CONNECTION_SHUTDOWN_FLAG_NONE,
        protocol == nullptr ? kErrorFrame : protocol->code());
  }

  void connected() {
    auto* stream_state = new ClientStream{shared_from_this(), true, {}};
    QUIC_STATUS status = api->StreamOpen(
        connection,
        QUIC_STREAM_OPEN_FLAG_NONE,
        [](HQUIC stream, void* context, QUIC_STREAM_EVENT* event) -> QUIC_STATUS {
          return client_stream_callback(stream, context, event);
        },
        stream_state,
        &control);
    if (QUIC_FAILED(status)) {
      delete stream_state;
      throw quic_error("StreamOpen(control)", status);
    }
    status = api->StreamStart(control, QUIC_STREAM_START_FLAG_IMMEDIATE);
    if (QUIC_FAILED(status)) {
      api->StreamClose(control);
      control = nullptr;
      delete stream_state;
      throw quic_error("StreamStart(control)", status);
    }
    send_bytes(api, control, encode_capabilities(Capabilities{}));
  }

  void control_frame(const ControlFrame& frame) {
    std::lock_guard lock(mutex);
    try {
      if (!capabilities) {
        if (frame.type != kFrameCapabilities) {
          throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", "server did not answer capabilities");
        }
        (void)decode_capabilities(frame.payload);
        capabilities = true;
        send_bytes(
            api,
            control,
            encode_status(Status{kStatusUnspecified, kConnectionLevel, 0, std::nullopt, 0}));
        send_bytes(
            api,
            control,
            encode_status(Status{kStatusPending, entity_id, 0, std::nullopt, 0}));
        open_entity_locked();
        return;
      }
      if (frame.type == kFrameStatus) {
        const Status status = decode_status(frame.payload);
        if (status.state == kStatusUnspecified && status.entity_id == kConnectionLevel &&
            !status.cursor) {
          return;
        }
        if (status.entity_id != entity_id) {
          throw ProtocolError(kErrorEntityInvalid, "PIPESTREAM_ENTITY_INVALID", "status references another entity");
        }
        if (!processing && status.state == kStatusProcessing) {
          processing = true;
          return;
        }
        if (processing && !complete && status.state == kStatusComplete) {
          complete = true;
          send_bytes(api, control, encode_checkpoint(Checkpoint{
              "entity-" + std::to_string(entity_id),
              1,
              next_entity_id(entity_id),
              std::nullopt,
              0,
              std::nullopt}));
          return;
        }
        throw ProtocolError(kErrorEntityInvalid, "PIPESTREAM_ENTITY_INVALID", "unexpected status progression");
      }
      if (frame.type == kFrameCheckpoint) {
        const Checkpoint checkpoint = decode_checkpoint(frame.payload);
        if (!complete || checkpoint.flags != kCheckpointAck ||
            checkpoint.checkpoint_id != "entity-" + std::to_string(entity_id) ||
            checkpoint.sequence_number != 1 ||
            checkpoint.checkpoint_entity_id != next_entity_id(entity_id)) {
          throw ProtocolError(
              kErrorEntityInvalid,
              "PIPESTREAM_ENTITY_INVALID",
              "invalid checkpoint acknowledgement");
        }
        checkpoint_acknowledged = true;
        send_bytes(api, control, encode_status(Status{
            kStatusUnspecified,
            kConnectionLevel,
            0,
            next_entity_id(entity_id),
            0}));
        send_bytes(api, control, encode_goaway(entity_id));
        return;
      }
      if (frame.type == kFrameGoaway) {
        if (!checkpoint_acknowledged || decode_goaway(frame.payload) != entity_id) {
          throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", "invalid GOAWAY acknowledgement");
        }
        protocol_complete.store(true);
        api->StreamShutdown(control, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, kErrorNoError);
        api->ConnectionShutdown(connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, kErrorNoError);
        return;
      }
      throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", "unexpected control frame type");
    } catch (const std::exception& error) {
      fail_locked(error.what(), dynamic_cast<const ProtocolError*>(&error));
    }
  }

  void open_entity_locked() {
    HQUIC stream = nullptr;
    auto* stream_state = new ClientStream{shared_from_this(), false, {}};
    QUIC_STATUS status = api->StreamOpen(
        connection,
        QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL,
        [](HQUIC value, void* context, QUIC_STREAM_EVENT* event) -> QUIC_STATUS {
          return client_stream_callback(value, context, event);
        },
        stream_state,
        &stream);
    if (QUIC_FAILED(status)) {
      delete stream_state;
      throw quic_error("StreamOpen(entity)", status);
    }
    status = api->StreamStart(stream, QUIC_STREAM_START_FLAG_IMMEDIATE);
    if (QUIC_FAILED(status)) {
      api->StreamClose(stream);
      delete stream_state;
      throw quic_error("StreamStart(entity)", status);
    }
    send_bytes(
        api,
        stream,
        encode_entity(entity_id, payload, content_type, parent_id),
        QUIC_SEND_FLAG_FIN);
  }
};

QUIC_STATUS QUIC_API client_stream_callback(HQUIC stream, void* context, QUIC_STREAM_EVENT* event) {
  auto* state = static_cast<ClientStream*>(context);
  try {
    switch (event->Type) {
      case QUIC_STREAM_EVENT_RECEIVE:
        for (std::uint32_t index = 0; index < event->RECEIVE.BufferCount; ++index) {
          const QUIC_BUFFER& buffer = event->RECEIVE.Buffers[index];
          if (state->input.size() + buffer.Length > kMaxControlFrame + 5) {
            throw ProtocolError(kErrorLimitExceeded, "PIPESTREAM_LIMIT_EXCEEDED", "control stream exceeds local limit");
          }
          state->input.insert(state->input.end(), buffer.Buffer, buffer.Buffer + buffer.Length);
        }
        if (state->control) {
          while (state->input.size() >= 5) {
            const std::size_t length = network_u32(state->input.data() + 1);
            if (length > kMaxControlFrame) {
              throw ProtocolError(kErrorLimitExceeded, "PIPESTREAM_LIMIT_EXCEEDED", "control frame exceeds local limit");
            }
            if (state->input.size() < length + 5) break;
            std::vector<std::uint8_t> frame_bytes(state->input.begin(), state->input.begin() + static_cast<std::ptrdiff_t>(length + 5));
            state->input.erase(state->input.begin(), state->input.begin() + static_cast<std::ptrdiff_t>(length + 5));
            state->session->control_frame(decode_control(frame_bytes));
          }
        }
        break;
      case QUIC_STREAM_EVENT_SEND_COMPLETE:
        delete static_cast<SendContext*>(event->SEND_COMPLETE.ClientContext);
        break;
      case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        throw ProtocolError(kErrorFrame, "PIPESTREAM_CONTROL_RESET", "peer aborted stream");
      case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        if (!event->SHUTDOWN_COMPLETE.AppCloseInProgress) state->session->api->StreamClose(stream);
        delete state;
        break;
      default:
        break;
    }
  } catch (const std::exception& error) {
    state->session->fail(error);
  }
  return QUIC_STATUS_SUCCESS;
}

QUIC_STATUS QUIC_API client_connection_callback(HQUIC connection, void* context, QUIC_CONNECTION_EVENT* event) {
  auto* state = static_cast<ClientConnection*>(context);
  try {
    switch (event->Type) {
      case QUIC_CONNECTION_EVENT_CONNECTED:
        state->session->connected();
        break;
      case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
        throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", "server opened an unexpected stream");
      case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        if (!state->session->protocol_complete.load()) {
          state->session->completion->fail("MsQuic transport closed before protocol completion");
        }
        break;
      case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        if (event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode != kErrorNoError) {
          state->session->completion->fail("server closed connection with an application error");
        }
        break;
      case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        if (state->session->protocol_complete.load() && !state->session->failed.load()) {
          state->session->completion->succeed();
        } else if (!state->session->failed.load()) {
          state->session->completion->fail("connection ended before Layer 0 completion");
        }
        if (!event->SHUTDOWN_COMPLETE.AppCloseInProgress) state->session->api->ConnectionClose(connection);
        delete state;
        break;
      default:
        break;
    }
  } catch (const std::exception& error) {
    state->session->fail(error);
  }
  return QUIC_STATUS_SUCCESS;
}

QUIC_SETTINGS settings(bool server) {
  QUIC_SETTINGS value{};
  value.IdleTimeoutMs = 30000;
  value.IsSet.IdleTimeoutMs = true;
  value.KeepAliveIntervalMs = 10000;
  value.IsSet.KeepAliveIntervalMs = true;
  if (server) {
    value.PeerBidiStreamCount = 1;
    value.IsSet.PeerBidiStreamCount = true;
    value.PeerUnidiStreamCount = 128;
    value.IsSet.PeerUnidiStreamCount = true;
    value.ServerResumptionLevel = QUIC_SERVER_NO_RESUME;
    value.IsSet.ServerResumptionLevel = true;
  }
  return value;
}

}  // namespace

void serve(const ServerOptions& options) {
  std::filesystem::create_directories(options.output_directory);
  Runtime runtime;
  const QUIC_SETTINGS server_settings = settings(true);
  Configuration configuration(runtime, server_settings);
  const std::string certificate = options.certificate.string();
  const std::string private_key = options.private_key.string();
  QUIC_CERTIFICATE_FILE certificate_file{private_key.c_str(), certificate.c_str()};
  QUIC_CREDENTIAL_CONFIG credential{};
  credential.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
  credential.CertificateFile = &certificate_file;
  check(runtime.api()->ConfigurationLoadCredential(configuration.handle(), &credential),
        "ConfigurationLoadCredential(server)");

  ServerRunner server{runtime.api(), configuration.handle(), options.output_directory, {}};
  HQUIC listener = nullptr;
  check(runtime.api()->ListenerOpen(
            runtime.registration(), server_listener_callback, &server, &listener),
        "ListenerOpen");
  try {
    QUIC_ADDR address{};
    if (!QuicAddrFromString(options.bind_host.c_str(), options.bind_port, &address)) {
      throw std::invalid_argument("invalid numeric bind address: " + options.bind_host);
    }
    check(runtime.api()->ListenerStart(listener, &kAlpnBuffer, 1, &address), "ListenerStart");
    std::uint32_t address_length = sizeof(address);
    check(runtime.api()->GetParam(
              listener, QUIC_PARAM_LISTENER_LOCAL_ADDRESS, &address_length, &address),
          "GetParam(listener address)");
    const std::string ready = options.bind_host + ":" + std::to_string(QuicAddrGetPort(&address));
    if (!options.ready_file.empty()) {
      std::ofstream file(options.ready_file, std::ios::trunc);
      if (!file) throw std::runtime_error("cannot write ready file");
      file << ready << '\n';
    }
    std::cout << "READY " << ready << '\n' << std::flush;
    if (options.once) {
      server.completion.wait(std::chrono::seconds(60));
    } else {
      for (;;) std::this_thread::sleep_for(std::chrono::hours(24));
    }
    runtime.api()->ListenerStop(listener);
    runtime.api()->ListenerClose(listener);
  } catch (...) {
    runtime.api()->ListenerStop(listener);
    runtime.api()->ListenerClose(listener);
    throw;
  }
}

void send(const ClientOptions& options) {
  if (options.entity_id == 0 || options.entity_id > kMaxEntityId) {
    throw std::invalid_argument("entity-id is reserved");
  }
  Runtime runtime;
  const QUIC_SETTINGS client_settings = settings(false);
  Configuration configuration(runtime, client_settings);
  const std::string ca = options.ca_certificate.string();
  QUIC_CREDENTIAL_CONFIG credential{};
  credential.Type = QUIC_CREDENTIAL_TYPE_NONE;
  credential.Flags = static_cast<QUIC_CREDENTIAL_FLAGS>(
      QUIC_CREDENTIAL_FLAG_CLIENT |
      QUIC_CREDENTIAL_FLAG_USE_TLS_BUILTIN_CERTIFICATE_VALIDATION |
      QUIC_CREDENTIAL_FLAG_SET_CA_CERTIFICATE_FILE);
  credential.CaCertificateFile = ca.c_str();
  check(runtime.api()->ConfigurationLoadCredential(configuration.handle(), &credential),
        "ConfigurationLoadCredential(client)");

  Completion completion;
  auto session = std::make_shared<ClientSession>(
      runtime.api(),
      &completion,
      options.entity_id,
      read_file(options.input),
      options.content_type,
      options.parent_id);
  auto* connection_state = new ClientConnection{session};
  HQUIC connection = nullptr;
  QUIC_STATUS status = runtime.api()->ConnectionOpen(
      runtime.registration(), client_connection_callback, connection_state, &connection);
  if (QUIC_FAILED(status)) {
    delete connection_state;
    throw quic_error("ConnectionOpen", status);
  }
  session->connection = connection;
  try {
    QUIC_ADDR remote{};
    if (!QuicAddrFromString(options.remote_host.c_str(), options.remote_port, &remote)) {
      throw std::invalid_argument("invalid numeric remote address: " + options.remote_host);
    }
    check(runtime.api()->SetParam(
              connection, QUIC_PARAM_CONN_REMOTE_ADDRESS, sizeof(remote), &remote),
          "SetParam(remote address)");
    check(runtime.api()->ConnectionStart(
              connection,
              configuration.handle(),
              QuicAddrGetFamily(&remote),
              options.server_name.c_str(),
              options.remote_port),
          "ConnectionStart");
    completion.wait(std::chrono::seconds(60));
    std::cout << "SENT " << options.entity_id << ' ' << session->payload.size() << '\n';
  } catch (...) {
    runtime.api()->ConnectionShutdown(connection, QUIC_CONNECTION_SHUTDOWN_FLAG_SILENT, kErrorFrame);
    throw;
  }
}

}  // namespace pipestream
