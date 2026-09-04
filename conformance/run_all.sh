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
"${bundle_command[@]}" exec cddl cddl/pipestream-layer0.cddl generate 1 >/dev/null

python3 conformance/generate_vectors.py --check
python3 conformance/validate_cddl.py
python3 conformance/verify_vectors.py
python3 -m unittest discover -s conformance -p 'test_*.py'

cargo fmt --manifest-path implementations/rust-quinn/Cargo.toml -- --check
cargo clippy --locked --all-targets --manifest-path implementations/rust-quinn/Cargo.toml -- -D warnings
cargo test --locked --manifest-path implementations/rust-quinn/Cargo.toml
cargo build --release --locked --manifest-path implementations/rust-quinn/Cargo.toml

mvn verify -q -f implementations/java-netty/pom.xml

cmake -S implementations/cpp-msquic -B implementations/cpp-msquic/build \
  -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build implementations/cpp-msquic/build -j 4
ctest --test-dir implementations/cpp-msquic/build --output-on-failure

python3 conformance/run_interop.py
python3 examples/run_all.py
