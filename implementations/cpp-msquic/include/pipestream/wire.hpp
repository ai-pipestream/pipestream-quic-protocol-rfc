#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace pipestream {

inline constexpr std::string_view kAlpn = "pipestream/1";
inline constexpr std::uint8_t kFrameStatus = 0x50;
inline constexpr std::uint8_t kFrameGoaway = 0x56;
inline constexpr std::uint8_t kFrameCapabilities = 0x80;
inline constexpr std::uint8_t kFrameCheckpoint = 0x81;

inline constexpr std::uint8_t kStatusUnspecified = 0;
inline constexpr std::uint8_t kStatusPending = 1;
inline constexpr std::uint8_t kStatusProcessing = 2;
inline constexpr std::uint8_t kStatusComplete = 3;
inline constexpr std::uint8_t kStatusFailed = 4;
inline constexpr std::uint8_t kCheckpointAck = 1;

inline constexpr std::uint32_t kMaxEntityId = 0xfffffffcu;
inline constexpr std::uint32_t kConnectionLevel = 0xffffffffu;
inline constexpr std::uint32_t kMaxWindow = 0x7ffffffeu;
inline constexpr std::size_t kMaxControlFrame = 1u << 20;
inline constexpr std::size_t kMaxEntityHeader = 1u << 16;
inline constexpr std::size_t kMaxPayload = 64u << 20;

inline constexpr std::uint64_t kErrorNoError = 0x00;
inline constexpr std::uint64_t kErrorIntegrity = 0x04;
inline constexpr std::uint64_t kErrorEntityInvalid = 0x05;
inline constexpr std::uint64_t kErrorLimitExceeded = 0x06;
inline constexpr std::uint64_t kErrorLayerUnsupported = 0x0c;
inline constexpr std::uint64_t kErrorFrame = 0x0d;

class ProtocolError final : public std::runtime_error {
 public:
  ProtocolError(std::uint64_t code, std::string name, std::string detail);

  [[nodiscard]] std::uint64_t code() const noexcept { return code_; }
  [[nodiscard]] const std::string& name() const noexcept { return name_; }

 private:
  std::uint64_t code_;
  std::string name_;
};

struct ControlFrame {
  std::uint8_t type;
  std::vector<std::uint8_t> payload;
};

struct Capabilities {
  bool layer0_core{true};
  bool layer1_recursive{false};
  bool layer2_resilience{false};
  std::uint32_t max_window_size{1024};
  std::uint8_t serialization_format{0};
  std::uint64_t keepalive_timeout_ms{30000};

  [[nodiscard]] Capabilities negotiate(const Capabilities& peer) const;
  bool operator==(const Capabilities&) const = default;
};

struct Status {
  std::uint8_t state;
  std::uint32_t entity_id;
  std::uint32_t scope_id{0};
  std::optional<std::uint32_t> cursor;
  std::uint8_t depth{0};

  bool operator==(const Status&) const = default;
};

struct EntityHeader {
  std::uint32_t entity_id;
  std::optional<std::uint32_t> parent_id;
  std::uint8_t layer{0};
  std::optional<std::string> content_type;
  std::optional<std::uint64_t> payload_length;
  std::optional<std::array<std::uint8_t, 32>> checksum;

  bool operator==(const EntityHeader&) const = default;
};

struct Checkpoint {
  std::string checkpoint_id;
  std::uint64_t sequence_number;
  std::uint32_t checkpoint_entity_id;
  std::optional<std::uint32_t> scope_id;
  std::uint8_t flags{0};
  std::optional<std::uint64_t> timeout_ms;

  bool operator==(const Checkpoint&) const = default;
};

struct Entity {
  EntityHeader header;
  std::vector<std::uint8_t> payload;
};

[[nodiscard]] std::vector<std::uint8_t> encode_control(
    std::uint8_t type, std::span<const std::uint8_t> payload);
[[nodiscard]] ControlFrame decode_control(std::span<const std::uint8_t> data);
[[nodiscard]] std::vector<std::uint8_t> encode_capabilities(const Capabilities& capabilities);
[[nodiscard]] Capabilities decode_capabilities(std::span<const std::uint8_t> payload);
[[nodiscard]] std::vector<std::uint8_t> encode_checkpoint(const Checkpoint& checkpoint);
[[nodiscard]] Checkpoint decode_checkpoint(std::span<const std::uint8_t> payload);
[[nodiscard]] std::vector<std::uint8_t> encode_status(const Status& status);
[[nodiscard]] Status decode_status(std::span<const std::uint8_t> payload);
[[nodiscard]] std::vector<std::uint8_t> encode_goaway(std::uint32_t last_entity_id);
[[nodiscard]] std::uint32_t decode_goaway(std::span<const std::uint8_t> payload);
[[nodiscard]] std::uint32_t next_entity_id(std::uint32_t current);
[[nodiscard]] std::vector<std::uint8_t> encode_entity(
    std::uint32_t entity_id,
    std::span<const std::uint8_t> payload,
    std::string_view content_type,
    std::optional<std::uint32_t> parent_id = std::nullopt);
[[nodiscard]] Entity decode_entity(std::span<const std::uint8_t> data);

}  // namespace pipestream
