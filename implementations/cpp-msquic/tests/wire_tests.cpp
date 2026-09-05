#include "pipestream/wire.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::vector<std::uint8_t> read(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("cannot read " + path.string());
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void decode_named(const std::string& name, const std::vector<std::uint8_t>& bytes) {
  if (name.starts_with("entity-")) {
    (void)pipestream::decode_entity(bytes);
    return;
  }
  const auto frame = pipestream::decode_control(bytes);
  if (name.starts_with("capabilities-") || name.starts_with("cbor-")) {
    if (frame.type != pipestream::kFrameCapabilities) throw std::runtime_error("wrong frame type");
    (void)pipestream::decode_capabilities(frame.payload);
  } else if (name.starts_with("status-")) {
    if (frame.type != pipestream::kFrameStatus) throw std::runtime_error("wrong frame type");
    (void)pipestream::decode_status(frame.payload);
  } else if (name.starts_with("goaway")) {
    if (frame.type != pipestream::kFrameGoaway) throw std::runtime_error("wrong frame type");
    (void)pipestream::decode_goaway(frame.payload);
  } else if (name.starts_with("checkpoint-")) {
    if (frame.type != pipestream::kFrameCheckpoint) throw std::runtime_error("wrong frame type");
    (void)pipestream::decode_checkpoint(frame.payload);
  }
}

}  // namespace

int main() {
  try {
    const std::filesystem::path root = PIPESTREAM_VECTOR_ROOT;
    std::ifstream optional(root / "optional-fields.tsv");
    if (!optional) throw std::runtime_error("cannot read optional-field vectors");
    std::string optional_row;
    std::getline(optional, optional_row);
    while (std::getline(optional, optional_row)) {
      std::istringstream columns(optional_row);
      std::string name, kind, expectation, hex;
      std::getline(columns, name, '\t');
      std::getline(columns, kind, '\t');
      std::getline(columns, expectation, '\t');
      std::getline(columns, hex, '\t');
      std::vector<std::uint8_t> bytes;
      for (std::size_t i = 0; i < hex.size(); i += 2) {
        bytes.push_back(static_cast<std::uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16)));
      }
      try {
        if (kind == "capabilities") (void)pipestream::decode_capabilities(bytes);
        else if (kind == "checkpoint") (void)pipestream::decode_checkpoint(bytes);
        else throw std::runtime_error("unknown vector kind");
        if (expectation == "invalid") throw std::runtime_error("accepted " + name);
      } catch (const pipestream::ProtocolError& error) {
        if (expectation == "valid" || error.name() != "PIPESTREAM_FRAME_ERROR") throw;
      }
    }
    std::ifstream index(root / "index.tsv");
    if (!index) throw std::runtime_error("cannot read vector index");
    std::string row;
    std::getline(index, row);
    while (std::getline(index, row)) {
      std::vector<std::string> fields;
      std::istringstream columns(row);
      std::string field;
      while (std::getline(columns, field, '\t')) fields.push_back(field);
      if (fields.size() < 4) throw std::runtime_error("malformed vector index row");
      const std::string& name = fields[0];
      const std::string& expectation = fields[2];
      try {
        decode_named(name, read(root / expectation / (name + ".bin")));
        if (expectation == "invalid") {
          throw std::runtime_error("accepted invalid vector " + name);
        }
      } catch (const pipestream::ProtocolError& error) {
        if (expectation == "valid") throw;
        if (error.name() != fields[3]) {
          throw std::runtime_error(
              name + " returned " + error.name() + " instead of " + fields[3]);
        }
      }
    }
    std::cout << "C++ wire vectors passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
