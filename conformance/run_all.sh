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

mvn install -q -Psealed-interop -f implementations/java-netty/pom.xml
mvn verify -q -f examples/java-to-rust/pom.xml

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
