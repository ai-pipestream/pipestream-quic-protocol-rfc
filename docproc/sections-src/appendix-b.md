# Appendix B: Complete Protocol Buffers Schema

This appendix provides an informational Protocol Buffers {{protobuf}}
schema for the document-processing profile. The canonical source is
maintained in the repository at
`proto/pipestream/data/v1/pipestream_data.proto`.

~~~~ protobuf
// Copyright 2026 PipeStream AI
//
// PipeStream Data Model
//
// Defines the document-processing profile for PipeStream entities.

edition = "2023";

package pipestream.data.v1;

import "google/protobuf/any.proto";
import "google/protobuf/struct.proto";

option features.enum_type = CLOSED;

message PipeDoc {
  string doc_id = 1;
  SearchMetadata search_metadata = 2;
  BlobBag blob_bag = 3;
  google.protobuf.Any structured_data = 4;
  map<string, ParsedMetadata> parsed_metadata = 5;
  SemanticProcessingResult semantic_result = 6;
  OwnershipContext ownership = 7;
  DocIdDerivation doc_id_derivation = 8;
}

message BlobBag {
  oneof blob_data {
    Blob blob = 1;
    Blobs blobs = 2;
  }
}

message Blobs {
  repeated Blob blobs = 1;
}

message Blob {
  string blob_id = 1;
  string drive_id = 2;
  oneof content {
    bytes data = 3;
    FileStorageReference storage_ref = 4;
  }
  string mime_type = 5;
  string filename = 6;
  int64 size_bytes = 8;
  string checksum = 9;
  ChecksumType checksum_type = 10;
}

message FileStorageReference {
  string provider = 1;
  string bucket = 2;
  string key = 3;
  string region = 4;
  map<string, string> attrs = 5;
  EncryptionMetadata encryption = 6;
}

message EncryptionMetadata {
  string algorithm = 1;
  string key_provider = 2;
  string key_id = 3;
  bytes wrapped_key = 4;
  bytes iv = 5;
  map<string, string> context = 6;
}

enum ChecksumType {
  CHECKSUM_TYPE_UNSPECIFIED = 0;
  CHECKSUM_TYPE_MD5 = 1;
  CHECKSUM_TYPE_SHA1 = 2;
  CHECKSUM_TYPE_SHA256 = 3;
  CHECKSUM_TYPE_SHA512 = 4;
}

message SemanticProcessingResult {
  repeated SemanticChunk chunks = 1;
  string chunking_strategy = 2;
  map<string, string> processing_metadata = 3;
}

message SemanticChunk {
  string chunk_id = 1;
  int64 chunk_number = 2;
  ChunkEmbedding embedding_info = 3;
  map<string, google.protobuf.Value> metadata = 4;
  repeated NLPAnnotation annotations = 5;
}

message ChunkEmbedding {
  string text_content = 1;
  repeated float vector = 2;
  string model_id = 3;
  int32 original_char_start_offset = 4;
  int32 original_char_end_offset = 5;
}

message NLPAnnotation {
  string type = 1;
  string label = 2;
  int32 start_offset = 3;
  int32 end_offset = 4;
  float confidence = 5;
  map<string, string> attributes = 6;
}

message ParsedMetadata {
  string parser_id = 1;
  map<string, google.protobuf.Value> fields = 2;
  repeated TableData tables = 3;
  string raw_output = 4;
}

message TableData {
  string table_id = 1;
  repeated string headers = 2;
  repeated TableRow rows = 3;
}

message TableRow {
  repeated string cells = 1;
}

message SearchMetadata {
  string title = 1;
  repeated string keywords = 2;
  string description = 3;
  map<string, string> custom_fields = 4;
}

message OwnershipContext {
  string tenant_id = 1;
  string owner_id = 2;
  repeated string acl = 3;
}

message DocIdDerivation {
  string strategy = 1;
  string source_field = 2;
  string hash_algorithm = 3;
}
~~~~
