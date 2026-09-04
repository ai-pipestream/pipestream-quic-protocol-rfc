#include "pipestream/wire.hpp"

#include <openssl/evp.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <sstream>
#include <string_view>
#include <utility>

namespace pipestream {
namespace {

[[noreturn]] void frame_error(std::string detail) {
  throw ProtocolError(kErrorFrame, "PIPESTREAM_FRAME_ERROR", std::move(detail));
}

[[noreturn]] void entity_error(std::string detail) {
  throw ProtocolError(kErrorEntityInvalid, "PIPESTREAM_ENTITY_INVALID", std::move(detail));
}

[[noreturn]] void integrity_error(std::string detail) {
  throw ProtocolError(kErrorIntegrity, "PIPESTREAM_INTEGRITY_ERROR", std::move(detail));
}

[[noreturn]] void limit_error(std::string detail) {
  throw ProtocolError(kErrorLimitExceeded, "PIPESTREAM_LIMIT_EXCEEDED", std::move(detail));
}

[[noreturn]] void layer_error(std::string detail) {
  throw ProtocolError(kErrorLayerUnsupported, "PIPESTREAM_LAYER_UNSUPPORTED", std::move(detail));
}

void append_u32(std::vector<std::uint8_t>& output, std::uint32_t value) {
  output.push_back(static_cast<std::uint8_t>(value >> 24));
  output.push_back(static_cast<std::uint8_t>(value >> 16));
  output.push_back(static_cast<std::uint8_t>(value >> 8));
  output.push_back(static_cast<std::uint8_t>(value));
}

std::uint32_t read_u32(std::span<const std::uint8_t> input, std::size_t offset) {
  if (input.size() < offset + 4) {
    frame_error("truncated uint32");
  }
  return (static_cast<std::uint32_t>(input[offset]) << 24) |
         (static_cast<std::uint32_t>(input[offset + 1]) << 16) |
         (static_cast<std::uint32_t>(input[offset + 2]) << 8) |
         static_cast<std::uint32_t>(input[offset + 3]);
}

void encode_head(std::vector<std::uint8_t>& output, std::uint8_t major, std::uint64_t value) {
  const auto prefix = static_cast<std::uint8_t>(major << 5);
  if (value < 24) {
    output.push_back(static_cast<std::uint8_t>(prefix | value));
  } else if (value <= 0xff) {
    output.insert(output.end(), {static_cast<std::uint8_t>(prefix | 24), static_cast<std::uint8_t>(value)});
  } else if (value <= 0xffff) {
    output.insert(output.end(), {static_cast<std::uint8_t>(prefix | 25),
                                 static_cast<std::uint8_t>(value >> 8),
                                 static_cast<std::uint8_t>(value)});
  } else if (value <= 0xffffffffu) {
    output.push_back(static_cast<std::uint8_t>(prefix | 26));
    append_u32(output, static_cast<std::uint32_t>(value));
  } else {
    output.push_back(static_cast<std::uint8_t>(prefix | 27));
    for (int shift = 56; shift >= 0; shift -= 8) {
      output.push_back(static_cast<std::uint8_t>(value >> shift));
    }
  }
}

void encode_uint(std::vector<std::uint8_t>& output, std::uint64_t value) {
  encode_head(output, 0, value);
}

void encode_text(std::vector<std::uint8_t>& output, std::string_view value) {
  encode_head(output, 3, value.size());
  output.insert(output.end(), value.begin(), value.end());
}

void encode_bytes(std::vector<std::uint8_t>& output, std::span<const std::uint8_t> value) {
  encode_head(output, 2, value.size());
  output.insert(output.end(), value.begin(), value.end());
}

void encode_bool(std::vector<std::uint8_t>& output, bool value) {
  output.push_back(value ? 0xf5 : 0xf4);
}

class Decoder {
 public:
  explicit Decoder(std::span<const std::uint8_t> input) : input_(input) {}

  [[nodiscard]] std::size_t position() const { return position_; }

  std::uint64_t map() {
    auto [major, value] = head();
    if (major != 5) {
      frame_error("CBOR value must be a map");
    }
    return value;
  }

  std::string key() {
    const std::size_t start = position_;
    std::string result = text();
    const std::span<const std::uint8_t> encoded = input_.subspan(start, position_ - start);
    if (previous_key_.has_value()) {
      const auto previous = *previous_key_;
      if (previous.size() > encoded.size() ||
          (previous.size() == encoded.size() &&
           !std::lexicographical_compare(previous.begin(), previous.end(), encoded.begin(), encoded.end()))) {
        frame_error("map keys are duplicate or not deterministic");
      }
    }
    previous_key_ = encoded;
    return result;
  }

  std::string text() {
    auto [major, length] = head();
    if (major != 3 || length > remaining()) {
      frame_error("expected bounded CBOR text");
    }
    const char* begin = reinterpret_cast<const char*>(input_.data() + position_);
    std::string result(begin, begin + static_cast<std::ptrdiff_t>(length));
    position_ += static_cast<std::size_t>(length);
    return result;
  }

  std::vector<std::uint8_t> bytes() {
    auto [major, length] = head();
    if (major != 2 || length > remaining()) {
      frame_error("expected bounded CBOR byte string");
    }
    std::vector<std::uint8_t> result(
        input_.begin() + static_cast<std::ptrdiff_t>(position_),
        input_.begin() + static_cast<std::ptrdiff_t>(position_ + length));
    position_ += static_cast<std::size_t>(length);
    return result;
  }

  std::uint64_t uint_value() {
    auto [major, value] = head();
    if (major != 0) {
      frame_error("expected CBOR unsigned integer");
    }
    return value;
  }

  bool boolean() {
    const std::uint8_t value = take();
    if (value == 0xf4) return false;
    if (value == 0xf5) return true;
    frame_error("expected CBOR boolean");
  }

 private:
  [[nodiscard]] std::size_t remaining() const { return input_.size() - position_; }

  std::uint8_t take() {
    if (position_ == input_.size()) {
      frame_error("truncated CBOR item");
    }
    return input_[position_++];
  }

  std::pair<std::uint8_t, std::uint64_t> head() {
    const std::uint8_t initial = take();
    const std::uint8_t major = initial >> 5;
    const std::uint8_t additional = initial & 0x1f;
    if (additional < 24) {
      return {major, additional};
    }
    std::size_t width = 0;
    std::uint64_t minimum = 0;
    switch (additional) {
      case 24: width = 1; minimum = 24; break;
      case 25: width = 2; minimum = 0x100; break;
      case 26: width = 4; minimum = 0x10000; break;
      case 27: width = 8; minimum = 0x100000000ULL; break;
      default: frame_error("indefinite or reserved CBOR length");
    }
    if (remaining() < width) {
      frame_error("truncated CBOR item");
    }
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < width; ++index) {
      value = (value << 8) | take();
    }
    if (value < minimum) {
      frame_error("non-deterministic CBOR integer width");
    }
    return {major, value};
  }

  std::span<const std::uint8_t> input_;
  std::size_t position_{0};
  std::optional<std::span<const std::uint8_t>> previous_key_;
};

std::array<std::uint8_t, 32> sha256(std::span<const std::uint8_t> input) {
  std::array<std::uint8_t, 32> output{};
  std::size_t length = output.size();
  if (EVP_Q_digest(nullptr, "SHA256", nullptr, input.data(), input.size(), output.data(), &length) != 1 ||
      length != output.size()) {
    throw std::runtime_error("OpenSSL SHA-256 failed");
  }
  return output;
}

std::vector<std::uint8_t> encode_capability_payload(const Capabilities& capabilities) {
  std::vector<std::uint8_t> output;
  encode_head(output, 5, 6);
  encode_text(output, "layer0-core");
  encode_bool(output, capabilities.layer0_core);
  encode_text(output, "max-window-size");
  encode_uint(output, capabilities.max_window_size);
  encode_text(output, "layer1-recursive");
  encode_bool(output, capabilities.layer1_recursive);
  encode_text(output, "layer2-resilience");
  encode_bool(output, capabilities.layer2_resilience);
  encode_text(output, "keepalive-timeout-ms");
  encode_uint(output, capabilities.keepalive_timeout_ms);
  encode_text(output, "serialization-format");
  encode_uint(output, capabilities.serialization_format);
  return output;
}

std::vector<std::uint8_t> encode_checkpoint_payload(const Checkpoint& checkpoint) {
  const std::size_t fields = 4 + checkpoint.scope_id.has_value() + checkpoint.timeout_ms.has_value();
  std::vector<std::uint8_t> output;
  encode_head(output, 5, fields);
  encode_text(output, "flags");
  encode_uint(output, checkpoint.flags);
  if (checkpoint.scope_id) {
    encode_text(output, "scope-id");
    encode_uint(output, *checkpoint.scope_id);
  }
  if (checkpoint.timeout_ms) {
    encode_text(output, "timeout-ms");
    encode_uint(output, *checkpoint.timeout_ms);
  }
  encode_text(output, "checkpoint-id");
  encode_text(output, checkpoint.checkpoint_id);
  encode_text(output, "sequence-number");
  encode_uint(output, checkpoint.sequence_number);
  encode_text(output, "checkpoint-entity-id");
  encode_uint(output, checkpoint.checkpoint_entity_id);
  return output;
}

std::vector<std::uint8_t> encode_entity_header(const EntityHeader& header) {
  std::size_t fields = 2;
  fields += header.parent_id.has_value();
  fields += header.content_type.has_value();
  fields += header.payload_length.has_value();
  fields += header.checksum.has_value();
  std::vector<std::uint8_t> output;
  encode_head(output, 5, fields);
  encode_text(output, "layer");
  encode_uint(output, header.layer);
  if (header.checksum) {
    encode_text(output, "checksum");
    encode_bytes(output, *header.checksum);
  }
  encode_text(output, "entity-id");
  encode_uint(output, header.entity_id);
  if (header.parent_id) {
    encode_text(output, "parent-id");
    encode_uint(output, *header.parent_id);
  }
  if (header.content_type) {
    encode_text(output, "content-type");
    encode_text(output, *header.content_type);
  }
  if (header.payload_length) {
    encode_text(output, "payload-length");
    encode_uint(output, *header.payload_length);
  }
  return output;
}

}  // namespace

ProtocolError::ProtocolError(std::uint64_t code, std::string name, std::string detail)
    : std::runtime_error(name + ": " + detail), code_(code), name_(std::move(name)) {}

Capabilities Capabilities::negotiate(const Capabilities& peer) const {
  if (!peer.layer0_core) {
    layer_error("Layer 0 is mandatory");
  }
  if (peer.max_window_size == 0 || peer.max_window_size > kMaxWindow) {
    limit_error("invalid max-window-size");
  }
  const bool layer1 = layer1_recursive && peer.layer1_recursive;
  return Capabilities{true,
                      layer1,
                      layer1 && layer2_resilience && peer.layer2_resilience,
                      std::min(max_window_size, peer.max_window_size),
                      0,
                      std::min(keepalive_timeout_ms, peer.keepalive_timeout_ms)};
}

std::vector<std::uint8_t> encode_control(
    std::uint8_t type, std::span<const std::uint8_t> payload) {
  if (payload.size() > std::numeric_limits<std::uint32_t>::max()) {
    limit_error("control frame exceeds uint32");
  }
  std::vector<std::uint8_t> output;
  output.reserve(5 + payload.size());
  output.push_back(type);
  append_u32(output, static_cast<std::uint32_t>(payload.size()));
  output.insert(output.end(), payload.begin(), payload.end());
  return output;
}

ControlFrame decode_control(std::span<const std::uint8_t> data) {
  if (data.size() < 5) {
    frame_error("truncated UCF header");
  }
  const std::size_t length = read_u32(data, 1);
  if (length > kMaxControlFrame) {
    limit_error("control frame exceeds local limit");
  }
  if (data.size() != length + 5) {
    frame_error("UCF length does not match payload");
  }
  return ControlFrame{data[0], std::vector<std::uint8_t>(data.begin() + 5, data.end())};
}

std::vector<std::uint8_t> encode_capabilities(const Capabilities& capabilities) {
  return encode_control(kFrameCapabilities, encode_capability_payload(capabilities));
}

Capabilities decode_capabilities(std::span<const std::uint8_t> payload) {
  Decoder decoder(payload);
  const std::uint64_t count = decoder.map();
  Capabilities result;
  bool has_layer0 = false;
  bool has_layer1 = false;
  bool has_layer2 = false;
  for (std::uint64_t index = 0; index < count; ++index) {
    const std::string key = decoder.key();
    if (key == "layer0-core") {
      result.layer0_core = decoder.boolean();
      has_layer0 = true;
    } else if (key == "layer1-recursive") {
      result.layer1_recursive = decoder.boolean();
      has_layer1 = true;
    } else if (key == "layer2-resilience") {
      result.layer2_resilience = decoder.boolean();
      has_layer2 = true;
    } else if (key == "max-window-size") {
      const auto value = decoder.uint_value();
      if (value > std::numeric_limits<std::uint32_t>::max()) limit_error("invalid max-window-size");
      result.max_window_size = static_cast<std::uint32_t>(value);
    } else if (key == "serialization-format") {
      const auto value = decoder.uint_value();
      if (value > std::numeric_limits<std::uint8_t>::max()) frame_error("serialization-format exceeds uint8");
      result.serialization_format = static_cast<std::uint8_t>(value);
    } else if (key == "keepalive-timeout-ms") {
      result.keepalive_timeout_ms = decoder.uint_value();
    } else {
      frame_error("unknown capabilities field " + key);
    }
  }
  if (decoder.position() != payload.size()) frame_error("trailing CBOR octets");
  if (!has_layer0 || !has_layer1 || !has_layer2) frame_error("missing mandatory capability boolean");
  if (!result.layer0_core) layer_error("Layer 0 is mandatory");
  if (result.max_window_size == 0 || result.max_window_size > kMaxWindow) {
    limit_error("invalid max-window-size");
  }
  if (encode_capability_payload(result) != std::vector<std::uint8_t>(payload.begin(), payload.end())) {
    frame_error("capabilities CBOR is not deterministic");
  }
  return result;
}

std::vector<std::uint8_t> encode_checkpoint(const Checkpoint& checkpoint) {
  return encode_control(kFrameCheckpoint, encode_checkpoint_payload(checkpoint));
}

Checkpoint decode_checkpoint(std::span<const std::uint8_t> payload) {
  Decoder decoder(payload);
  const std::uint64_t count = decoder.map();
  Checkpoint result{};
  bool has_checkpoint_id = false;
  bool has_sequence = false;
  bool has_entity = false;
  for (std::uint64_t index = 0; index < count; ++index) {
    const std::string key = decoder.key();
    if (key == "checkpoint-id") {
      result.checkpoint_id = decoder.text();
      has_checkpoint_id = true;
    } else if (key == "sequence-number") {
      result.sequence_number = decoder.uint_value();
      has_sequence = true;
    } else if (key == "checkpoint-entity-id") {
      const auto value = decoder.uint_value();
      if (value > std::numeric_limits<std::uint32_t>::max()) {
        entity_error("checkpoint-entity-id exceeds uint32");
      }
      result.checkpoint_entity_id = static_cast<std::uint32_t>(value);
      has_entity = true;
    } else if (key == "scope-id") {
      const auto value = decoder.uint_value();
      if (value > std::numeric_limits<std::uint32_t>::max()) frame_error("scope-id exceeds uint32");
      result.scope_id = static_cast<std::uint32_t>(value);
    } else if (key == "flags") {
      const auto value = decoder.uint_value();
      if (value > std::numeric_limits<std::uint8_t>::max()) frame_error("checkpoint flags exceed uint8");
      result.flags = static_cast<std::uint8_t>(value);
    } else if (key == "timeout-ms") {
      result.timeout_ms = decoder.uint_value();
    } else {
      frame_error("unknown checkpoint field " + key);
    }
  }
  if (decoder.position() != payload.size()) frame_error("trailing checkpoint CBOR octets");
  if (!has_checkpoint_id || result.checkpoint_id.empty() || result.checkpoint_id.size() > 256) {
    frame_error("invalid checkpoint-id");
  }
  if (!has_sequence) frame_error("missing sequence-number");
  if (!has_entity) frame_error("missing checkpoint-entity-id");
  if (result.checkpoint_entity_id == 0 || result.checkpoint_entity_id > kMaxEntityId) {
    entity_error("invalid checkpoint-entity-id");
  }
  if (result.scope_id && *result.scope_id != 0) layer_error("checkpoint scope requires Layer 1");
  if (result.flags > kCheckpointAck) frame_error("unknown checkpoint flags");
  if (encode_checkpoint_payload(result) != std::vector<std::uint8_t>(payload.begin(), payload.end())) {
    frame_error("checkpoint CBOR is not deterministic");
  }
  return result;
}

std::vector<std::uint8_t> encode_status(const Status& status) {
  if (status.depth > 7) entity_error("depth exceeds 7");
  std::uint32_t word = (1u << 28) | ((static_cast<std::uint32_t>(status.state) & 0xf) << 24) |
                       (static_cast<std::uint32_t>(status.depth) << 19);
  if (status.cursor) word |= 1u << 22;
  std::vector<std::uint8_t> payload;
  payload.reserve(status.cursor ? 20 : 16);
  append_u32(payload, word);
  append_u32(payload, status.entity_id);
  append_u32(payload, status.scope_id);
  append_u32(payload, 0);
  if (status.cursor) append_u32(payload, *status.cursor);
  return encode_control(kFrameStatus, payload);
}

Status decode_status(std::span<const std::uint8_t> payload) {
  if (payload.size() != 16 && payload.size() != 20) frame_error("invalid STATUS payload length");
  const std::uint32_t word = read_u32(payload, 0);
  if ((word >> 28) != 1) layer_error("unsupported STATUS version");
  if ((word & (1u << 23)) != 0) frame_error("Layer 0 STATUS cannot carry extensions");
  const bool has_cursor = (word & (1u << 22)) != 0;
  if (has_cursor != (payload.size() == 20)) frame_error("STATUS cursor flag and length disagree");
  const std::uint8_t state = static_cast<std::uint8_t>((word >> 24) & 0xf);
  const std::uint8_t depth = static_cast<std::uint8_t>((word >> 19) & 0x7);
  const std::uint32_t entity_id = read_u32(payload, 4);
  const std::uint32_t scope_id = read_u32(payload, 8);
  if (depth != 0 || scope_id != 0) layer_error("scope fields require Layer 1");
  if (state >= 8) layer_error("status requires Layer 2");
  if (state == kStatusUnspecified && entity_id != kConnectionLevel) {
    entity_error("UNSPECIFIED is connection-level only");
  }
  const auto cursor = has_cursor ? std::optional{read_u32(payload, 16)} : std::nullopt;
  if (cursor &&
      (state != kStatusUnspecified || entity_id != kConnectionLevel || scope_id != 0 || depth != 0)) {
    entity_error("cursor update must be connection-level");
  }
  return Status{state, entity_id, scope_id, cursor, depth};
}

std::vector<std::uint8_t> encode_goaway(std::uint32_t last_entity_id) {
  std::vector<std::uint8_t> payload(4, 0);
  append_u32(payload, last_entity_id);
  return encode_control(kFrameGoaway, payload);
}

std::uint32_t decode_goaway(std::span<const std::uint8_t> payload) {
  if (payload.size() != 8) frame_error("invalid GOAWAY payload length");
  return read_u32(payload, 4);
}

std::uint32_t next_entity_id(std::uint32_t current) {
  if (current == 0 || current > kMaxEntityId) entity_error("entity-id is reserved");
  return current == kMaxEntityId ? 1 : current + 1;
}

std::vector<std::uint8_t> encode_entity(
    std::uint32_t entity_id,
    std::span<const std::uint8_t> payload,
    std::string_view content_type,
    std::optional<std::uint32_t> parent_id) {
  if (entity_id == 0 || entity_id > kMaxEntityId) entity_error("entity-id is reserved");
  if (parent_id && (*parent_id == 0 || *parent_id > kMaxEntityId)) {
    entity_error("parent-id is reserved or invalid");
  }
  if (payload.size() > kMaxPayload) limit_error("entity payload exceeds local limit");
  EntityHeader header{
      entity_id, parent_id, 0, std::string(content_type), payload.size(), sha256(payload)};
  std::vector<std::uint8_t> encoded_header = encode_entity_header(header);
  std::vector<std::uint8_t> output;
  output.reserve(4 + encoded_header.size() + payload.size());
  append_u32(output, static_cast<std::uint32_t>(encoded_header.size()));
  output.insert(output.end(), encoded_header.begin(), encoded_header.end());
  output.insert(output.end(), payload.begin(), payload.end());
  return output;
}

Entity decode_entity(std::span<const std::uint8_t> data) {
  if (data.size() < 4) frame_error("truncated entity header length");
  const std::size_t header_length = read_u32(data, 0);
  if (header_length > kMaxEntityHeader) limit_error("entity header exceeds local limit");
  if (data.size() < 4 + header_length) frame_error("truncated entity header");
  const auto encoded = data.subspan(4, header_length);
  const auto payload = data.subspan(4 + header_length);
  if (payload.size() > kMaxPayload) limit_error("entity payload exceeds local limit");
  Decoder decoder(encoded);
  const std::uint64_t count = decoder.map();
  EntityHeader header{};
  bool has_entity = false;
  bool has_layer = false;
  for (std::uint64_t index = 0; index < count; ++index) {
    const std::string key = decoder.key();
    if (key == "entity-id") {
      const auto value = decoder.uint_value();
      if (value > std::numeric_limits<std::uint32_t>::max()) entity_error("entity-id exceeds uint32");
      header.entity_id = static_cast<std::uint32_t>(value);
      has_entity = true;
    } else if (key == "parent-id") {
      const auto value = decoder.uint_value();
      if (value > std::numeric_limits<std::uint32_t>::max()) entity_error("parent-id exceeds uint32");
      header.parent_id = static_cast<std::uint32_t>(value);
    } else if (key == "layer") {
      const auto value = decoder.uint_value();
      if (value > std::numeric_limits<std::uint8_t>::max()) entity_error("layer exceeds uint8");
      header.layer = static_cast<std::uint8_t>(value);
      has_layer = true;
    } else if (key == "content-type") {
      header.content_type = decoder.text();
    } else if (key == "payload-length") {
      header.payload_length = decoder.uint_value();
    } else if (key == "checksum") {
      auto checksum = decoder.bytes();
      if (checksum.size() != 32) integrity_error("checksum must contain 32 octets");
      std::array<std::uint8_t, 32> value{};
      std::copy(checksum.begin(), checksum.end(), value.begin());
      header.checksum = value;
    } else {
      frame_error("unsupported Layer 0 entity field " + key);
    }
  }
  if (decoder.position() != encoded.size()) frame_error("trailing entity header CBOR octets");
  if (!has_entity) entity_error("entity-id is absent");
  if (!has_layer) entity_error("layer is absent");
  if (header.entity_id == 0 || header.entity_id > kMaxEntityId) entity_error("entity-id is reserved");
  if (header.parent_id && (*header.parent_id == 0 || *header.parent_id > kMaxEntityId)) {
    entity_error("parent-id is reserved or invalid");
  }
  if (header.layer > 3) entity_error("layer must be 0 through 3");
  if (header.payload_length && *header.payload_length != payload.size()) entity_error("payload-length mismatch");
  if (header.checksum && *header.checksum != sha256(payload)) integrity_error("checksum mismatch");
  if (encode_entity_header(header) != std::vector<std::uint8_t>(encoded.begin(), encoded.end())) {
    frame_error("entity header CBOR is not deterministic");
  }
  return Entity{std::move(header), std::vector<std::uint8_t>(payload.begin(), payload.end())};
}

}  // namespace pipestream
