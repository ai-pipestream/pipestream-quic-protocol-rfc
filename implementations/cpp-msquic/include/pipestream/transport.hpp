#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace pipestream {

struct ServerOptions {
  std::string bind_host{"127.0.0.1"};
  std::uint16_t bind_port{0};
  std::filesystem::path certificate;
  std::filesystem::path private_key;
  std::filesystem::path output_directory;
  std::filesystem::path ready_file;
  bool once{false};
};

struct ClientOptions {
  std::string remote_host;
  std::uint16_t remote_port;
  std::filesystem::path ca_certificate;
  std::string server_name{"localhost"};
  std::uint32_t entity_id;
  std::filesystem::path input;
  std::string content_type{"application/octet-stream"};
  std::optional<std::uint32_t> parent_id;
};

void serve(const ServerOptions& options);
void send(const ClientOptions& options);

}  // namespace pipestream
