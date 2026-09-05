# PipeStream Protocol

**PipeStream: A Recursive Entity Streaming Protocol for Distributed Document Processing over QUIC**

**Internet-Draft: draft-krickert-pipestream**

## Overview

PipeStream is a proposed application protocol over QUIC for recursive work
decomposition, entity streaming, completion barriers, and durable continuation
references. The draft and implementations are under development. They are not
an approved IETF standard or a fully conformant production implementation.
See [draft-04 readiness](docs/standards/draft04-readiness.md) for tested changes
and the remaining interoperability and security work.

Draft -04 now defines supported/required extension negotiation, implemented
independently in Rust, Java, and C++. Unknown requirements fail CONNECT;
optional unknown identifiers are not activated. Sealed work sets and
their durable producer/session binding are now available in Rust through
the opt-in private-use `sealed-work-sets-v1` profile (Section 9.8).
Authenticated resilience, bidirectional producers, and independent Java/C++
implementations of that profile remain unfinished.

The Rust durable service also supports negotiated mutual-TLS session binding:
certificate-mapped principals, retained authority/owner records, and session
revocation. This is the authentication prerequisite for recovery, not the
retained recovery-request protocol. Durable attempt
fences now protect result publication, and callbacks run outside database
transactions. Receive payloads are now incrementally spooled to bounded
temporary files and processed through readers. The service submits typed jobs
to a transactionally bounded queue and dispatches processing, rehydration, and
resume callbacks in bounded workers, independently of connection control handling.
Retained payloads are reopened and verified before interrupted work is executed.
Permanent storage quotas and storage-stall handling remain unfinished.
The full remaining
goal is tracked in [the implementation plan](docs/standards/recovery-execution-java-plan.md).

## Authoring Workflow

This repository uses a modular authoring workflow for IETF drafts. The monolithic draft is treated as a build artifact and is not checked into the repository.

### Source Structure

- **`sections-src/`**: **The Source of Truth.** Individual Markdown files for each RFC section. Edit these files directly.
- **`draft-template.md`**: The master kramdown-rfc template that includes all sections in the correct order.
- **`cddl/`**: Machine-readable serialized-message CDDL, checked against Appendix C.
- **`test-vectors/`**: Checked-in golden valid and invalid wire inputs with named expected refusals.
- **`conformance/`**: Vector checks and the black-box client/server interoperability runner.
- **`implementations/`**: Independent Java/Netty, Rust/Quinn, and C++/MsQuic libraries and executables.
- **`examples/`**: Language-native Java and Rust applications that use the reusable implementations.
- **`proto/`**: Non-normative Protocol Buffers definitions used by implementation tooling. Not part of the Internet-Draft; the specification's normative schemas use CDDL (Appendix C). An alternative serialization format may be registered separately via the PipeStream Serialization Formats registry.

## Reference Suite

The Layer 0 reference suite is intentionally polyglot. Each implementation owns its codec and protocol state machine; no implementation imports protocol code from another. The shared artifacts are the specification, CDDL, and language-neutral binary corpus.

The common standalone interface supports `serve` and `send`. A successful transfer negotiates capabilities, sends one immutable Entity Stream, validates SHA-256, crosses a CHECKPOINT request/acknowledgement barrier, advances the connection cursor, and completes a GOAWAY exchange. TLS 1.3 and ALPN `pipestream/1` are mandatory and 0-RTT is disabled.

Run the complete suite after installing the prerequisites below and the language toolchains:

```bash
bundle install
./conformance/run_all.sh
```

That command checks frozen vectors, runs every implementation and example's
tests, builds the three servers and language-native applications, executes all
nine black-box client/server pairings, and runs the three external scenarios.
See [`conformance/README.md`](conformance/README.md) for the command contract and
[`examples/README.md`](examples/README.md) for the application sources.

Dependency versions are reproducible: Ruby dependencies are exact in
`Gemfile.lock`, Rust dependencies in `Cargo.lock`, Java dependencies and build
plugins in `pom.xml`, and MsQuic at an immutable Git tag in `CMakeLists.txt`.
The direct dependencies were checked against their upstream registries on
2026-09-04; the reference suite uses the latest compatible stable releases at
that point. Rustls remains on the latest 0.23 release required by Quinn rather
than the 0.24 development series.

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

#### 2. Structured Metadata
Use appropriate syntax highlighters for schema blocks:
```markdown
~~~~ cddl
example = {
  id: uint
}
~~~~
```

## Build Instructions

### 1. Prerequisites

You need the following tools installed:

- **Ruby and Bundler**: For the pinned `kramdown-rfc` and CDDL validator gems
- **xml2rfc 3.34.0 via uv**: External IETF document rendering only
- **idnits**: For final validation

```bash
# Install toolchain (macOS example)
brew install ruby idnits
gem install bundler
bundle install
uv tool install xml2rfc==3.34.0
```

There are no checked-in Python sources, and the reference implementations,
vector checks, interoperability matrix, and examples do not invoke Python.
`xml2rfc` is an external IETF authoring tool used only by `build.sh` to render
the draft. It is not part of the protocol or its conformance evidence.

### 2. Generating the Draft

To build all formats (XML, TXT, HTML) in one pass:

```bash
./build.sh core 04
```

The script runs the pinned `kramdown-rfc` and `xml2rfc` toolchain, emits XML,
TXT, and HTML, and finishes with `idnits` validation. Generated drafts are
ignored build artifacts.

### 3. Validation

Always run `idnits` on the generated `.txt` file before submitting to ensure there are no formatting errors or non-ASCII characters:

```bash
idnits --verbose draft-krickert-pipestream-04.txt
```

## Submission

Submit the generated **`.xml`** file to the IETF Datatracker:
[https://datatracker.ietf.org/submit/](https://datatracker.ietf.org/submit/)

---

## Repository Contents

- **`REFERENCE_IMPLEMENTATION.md`**: Reference-suite status plus informative guidance on algorithms.
- **`OVERVIEW.md`**: High-level architectural summary.
- **`advocacy/`**: IETF process materials (prior-art survey, DISPATCH kit, ANRW paper draft, submission checklist). Not part of any Internet-Draft.
- **`build/`**: (Ignored) Temporary build artifacts.

## Authors

- **Kristian Rickert** (PipeStream AI) — <kristian.rickert@pipestream.ai>

## Status

This is an active Internet-Draft targeting IETF standards track.
