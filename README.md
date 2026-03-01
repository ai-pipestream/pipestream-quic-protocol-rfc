# PipeStream Protocol

**PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing over QUIC**

**Internet-Draft: draft-krickert-pipestream**

## Overview

PipeStream is a recursive entity streaming protocol designed for high-performance distributed document processing over QUIC transport. It implements a scatter-gather pattern where documents are "dehydrated" (scattered) into constituent entities, processed in parallel across distributed nodes, and "rehydrated" (gathered) back into complete processed documents with strong consistency guarantees.

## Authoring Workflow

This repository uses a modular authoring workflow for IETF drafts. The monolithic draft is treated as a build artifact and is not checked into the repository.

### Source Structure

- **`sections-src/`**: **The Source of Truth.** Individual Markdown files for each RFC section. Edit these files directly.
- **`draft-template.md`**: The master kramdown-rfc template that includes all sections in the correct order.
- **`proto/`**: Canonical Protobuf definitions (Edition 2023). Inline protobuf blocks in the spec MUST match these files.

### Styling Conventions for Rendering

To ensure the IETF Datatracker renders diagrams and code correctly in both HTML and Plain Text, use the following fences:

#### 1. Packet Diagrams (ASCII Art)
Always use the `ascii-art` type to force monospaced rendering:
```markdown
~~~~
    0 1 2 3
   +-+-+-+-+
   | Data  |
   +-+-+-+-+
~~~~
{: type="ascii-art"}
```

#### 2. Protobuf/Source Code
Use the `protobuf` type for schema blocks:
```markdown
~~~~ protobuf
message Example {
  uint32 id = 1;
}
~~~~
```

## Build Instructions

### 1. Prerequisites

You need the following tools installed:

- **Ruby**: For `kramdown-rfc`
- **Python/uv**: For `xml2rfc`
- **idnits**: For final validation

```bash
# Install toolchain (macOS example)
brew install ruby idnits
gem install kramdown-rfc
uv tool install xml2rfc
```

### 2. Generating the Draft

To build all formats (XML, TXT, HTML) in one pass:

```bash
# 1. Convert Markdown source to IETF XML v3
kdrfc draft-template.md

# 2. Rename to the official draft name (e.g., version -01)
mv draft-template.xml draft-krickert-pipestream-01.xml

# 3. Generate TXT and HTML versions from the XML
xml2rfc draft-krickert-pipestream-01.xml --text --html
```

### 3. Validation

Always run `idnits` on the generated `.txt` file before submitting to ensure there are no formatting errors or non-ASCII characters:

```bash
idnits --verbose draft-krickert-pipestream-01.txt
```

## Submission

Submit the generated **`.xml`** file to the IETF Datatracker:
[https://datatracker.ietf.org/submit/](https://datatracker.ietf.org/submit/)

---

## Repository Contents

- **`REFERENCE_IMPLEMENTATION.md`**: Informative guidance on algorithms (Fibonacci heaps, Merkle trees).
- **`OVERVIEW.md`**: High-level architectural summary.
- **`build/`**: (Ignored) Temporary build artifacts.

## Authors

- **Kristian Rickert** (PipeStream AI) — <kristian.rickert@pipestream.ai>

## Status

This is an active Internet-Draft targeting IETF standards track.
