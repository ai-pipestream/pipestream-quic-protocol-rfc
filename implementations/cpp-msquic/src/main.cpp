#include "pipestream/transport.hpp"

#include <charconv>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

struct Address {
  std::string host;
  std::uint16_t port;
};

void usage() {
  std::cout
      << "pipestream-msquic serve --cert FILE --key FILE --output-dir DIR "
         "[--bind HOST:PORT] [--ready-file FILE] [--once]\n"
      << "pipestream-msquic send --connect HOST:PORT --ca FILE --entity-id UINT32 "
         "--input FILE [--server-name NAME] [--content-type MIME] [--parent-id UINT32]\n";
}

std::map<std::string, std::string> parse_options(int argc, char** argv) {
  std::map<std::string, std::string> options;
  for (int index = 2; index < argc; ++index) {
    const std::string argument = argv[index];
    if (!argument.starts_with("--")) {
      throw std::invalid_argument("expected named option, found: " + argument);
    }
    const std::string key = argument.substr(2);
    if (key == "once") {
      options[key] = "true";
    } else {
      if (++index == argc) throw std::invalid_argument("missing value for --" + key);
      options[key] = argv[index];
    }
  }
  return options;
}

std::string required(const std::map<std::string, std::string>& options, const std::string& name) {
  const auto found = options.find(name);
  if (found == options.end() || found->second.empty()) {
    throw std::invalid_argument("missing --" + name);
  }
  return found->second;
}

std::uint32_t unsigned_value(std::string_view value, std::string_view name) {
  std::uint32_t output = 0;
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), output);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument("invalid --" + std::string(name));
  }
  return output;
}

Address address(const std::string& value) {
  const std::size_t separator = value.rfind(':');
  if (separator == std::string::npos || separator == 0 || separator + 1 == value.size()) {
    throw std::invalid_argument("address must be host:port: " + value);
  }
  const std::uint32_t port = unsigned_value(std::string_view(value).substr(separator + 1), "port");
  if (port > std::numeric_limits<std::uint16_t>::max()) {
    throw std::invalid_argument("port exceeds uint16");
  }
  return Address{value.substr(0, separator), static_cast<std::uint16_t>(port)};
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc < 2 || std::string_view(argv[1]) == "--help") {
      usage();
      return 0;
    }
    const std::string command = argv[1];
    const auto options = parse_options(argc, argv);
    if (command == "serve") {
      const Address bind = address(options.contains("bind") ? options.at("bind") : "127.0.0.1:0");
      pipestream::serve(pipestream::ServerOptions{
          bind.host,
          bind.port,
          required(options, "cert"),
          required(options, "key"),
          required(options, "output-dir"),
          options.contains("ready-file") ? options.at("ready-file") : "",
          options.contains("once")});
      return 0;
    }
    if (command == "send") {
      const Address remote = address(required(options, "connect"));
      pipestream::send(pipestream::ClientOptions{
          remote.host,
          remote.port,
          required(options, "ca"),
          options.contains("server-name") ? options.at("server-name") : "localhost",
          unsigned_value(required(options, "entity-id"), "entity-id"),
          required(options, "input"),
          options.contains("content-type") ? options.at("content-type") : "application/octet-stream",
          options.contains("parent-id")
              ? std::optional{unsigned_value(options.at("parent-id"), "parent-id")}
              : std::nullopt});
      return 0;
    }
    throw std::invalid_argument("unknown command: " + command);
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
