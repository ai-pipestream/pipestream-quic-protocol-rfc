#!/usr/bin/env bash
set -euo pipefail

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repository_root"

if command -v bundle >/dev/null 2>&1; then
  bundle_command=(bundle)
elif command -v bundle3.3 >/dev/null 2>&1; then
  bundle_command=(bundle3.3)
else
  echo "Bundler is required. Install it with: gem install bundler" >&2
  exit 1
fi

"${bundle_command[@]}" check
cargo fmt --all --manifest-path implementations/rust-quinn/Cargo.toml -- --check
cargo clippy --locked --workspace --all-targets --manifest-path implementations/rust-quinn/Cargo.toml -- -D warnings
cargo test --locked --workspace --manifest-path implementations/rust-quinn/Cargo.toml
cargo build --release --locked --workspace --manifest-path implementations/rust-quinn/Cargo.toml
implementations/rust-quinn/target/release/pipestream-conformance verify
implementations/rust-quinn/target/release/pipestream-conformance modelcheck --depth 32 --max-states 1000000

java_maven_repository=$(bash implementations/java-netty/transport/build.sh)
mvn install -q -Psealed-interop "-Dmaven.repo.local=$java_maven_repository" \
  -f implementations/java-netty/pom.xml
mvn verify -q "-Dmaven.repo.local=$java_maven_repository" -f examples/java-to-rust/pom.xml

# Neutral durable failure/resource matrix (assignment B). Opt-in: the full
# both-direction matrix takes about an hour, so it runs only with
# PIPESTREAM_DURABLE_ACCEPTANCE=1. Acceptance mode (no --dev) FAILs on any
# row failure; the four --waive flags name the two missing subject
# capabilities and the two per-direction gaps decided at milestone 20
# (conformance/results/async-neutral-v2/handoff.md section 3k). On the
# shared host point PIPESTREAM_DURABLE_STORE and TMPDIR at the root drive
# (never /work, never /tmp) and run the script under BENCHMARK.lock.
if [[ "${PIPESTREAM_DURABLE_ACCEPTANCE:-0}" == "1" ]]; then
  durable_jar=$(ls implementations/java-netty/target/pipestream-quic-netty-*-all.jar)
  durable_store=${PIPESTREAM_DURABLE_STORE:-"$repository_root/implementations/rust-quinn/target/durable-runs"}
  implementations/rust-quinn/target/release/pipestream-conformance durable \
    --rust-bin implementations/rust-quinn/target/release/pipestream-quinn \
    --java-jar "$durable_jar" \
    --artifacts "$durable_store" \
    --archive conformance/results/async-neutral-v2/runs \
    --waive "g7-unsafe-clock-refusal=no fixture clock on either subject; interface-v1 has no clock-set boundary" \
    --waive "g7-cleanup-interrupted-refund=no cleanup boundary in interface-v1; adding one is an interface revision" \
    --waive "g3-store-ownership:java-client/rust-server=the client subject never owns the store" \
    --waive "g4-revocation-vs-publication:rust-client/java-server=no Java operator revoke command"
fi

cmake -S implementations/cpp-msquic -B implementations/cpp-msquic/build \
  -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build implementations/cpp-msquic/build -j 4
ctest --test-dir implementations/cpp-msquic/build --output-on-failure

for example in rust-to-cpp-recovery three-node-scatter; do
  manifest="examples/${example}/Cargo.toml"
  cargo fmt --manifest-path "$manifest" -- --check
  cargo clippy --locked --all-targets --manifest-path "$manifest" -- -D warnings
  cargo test --locked --manifest-path "$manifest"
  cargo build --release --locked --manifest-path "$manifest"
done

implementations/rust-quinn/target/release/pipestream-conformance interop
implementations/rust-quinn/target/release/pipestream-conformance extensions
implementations/rust-quinn/target/release/pipestream-conformance recursive
implementations/rust-quinn/target/release/pipestream-conformance examples
