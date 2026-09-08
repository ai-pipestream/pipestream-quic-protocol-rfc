#!/usr/bin/env bash
# Build the transport extension from pinned upstream sources and reviewed patches.
set -euo pipefail

if [[ $# -ne 0 ]]; then
    printf '%s\n' 'Usage: bash build.sh (no arguments)' >&2
    exit 2
fi

transport_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
reference_root=${PIPESTREAM_REFERENCE_CODE_ROOT:-/work/reference-code}
case "$(uname -s):$(uname -m)" in
    Linux:x86_64) ;;
    *) printf '%s\n' 'This build entry point currently supports Linux x86_64 only.' >&2; exit 2 ;;
esac
if [[ ! -d "$reference_root" ]]; then
    printf 'Reference-code directory does not exist: %s\n' "$reference_root" >&2
    exit 2
fi
for prerequisite in git mvn cargo cmake make cc c++ perl go sha256sum; do
    command -v "$prerequisite" >/dev/null
done
(
    cd "$transport_dir"
    sha256sum --check SHA256SUMS
)

# Each invocation owns a new directory. No retained checkout or global Maven
# cache is overwritten or deleted, including on failure.
build_root=$(mktemp -d "$reference_root/pipestream-quic-build.XXXXXXXX")
printf 'Transport build directory: %s\n' "$build_root"
netty_source="$build_root/netty"
quiche_source="$netty_source/codec-native-quic/target/quiche-source"
maven_repository="$build_root/maven-repository"

fetch_revision() {
    local repository=$1 revision=$2 destination=$3
    git init --quiet "$destination"
    git -C "$destination" remote add upstream "$repository"
    git -C "$destination" fetch --depth 1 upstream "$revision"
    git -C "$destination" switch --detach FETCH_HEAD
    [[ "$(git -C "$destination" rev-parse HEAD)" == "$revision" ]]
}

fetch_revision https://github.com/netty/netty.git \
    e0789d32c72f46fd2e7c99b6fdbbf7e2f4409e44 "$netty_source"
git -C "$netty_source" apply --check "$transport_dir/netty.patch"
git -C "$netty_source" apply "$transport_dir/netty.patch"

fetch_revision https://github.com/cloudflare/quiche.git \
    4f347477006bf7f928335d28f05056013f70b87e "$quiche_source"
git -C "$quiche_source" apply --check "$transport_dir/quiche.patch"
git -C "$quiche_source" apply "$transport_dir/quiche.patch"
cp "$transport_dir/quiche-Cargo.lock" "$quiche_source/Cargo.lock"

# The native upstream build owns (and deletes) its target source checkout.
# Never pass a retained reference-code checkout as quicheSourceDir or
# boringsslSourceDir. Its BoringSSL build uses its own pinned revision, which
# differs from the quiche repository's submodule pin.
(
    cd "$netty_source"
    mvn -B -ntp -pl codec-classes-quic,codec-native-quic \
        -DskipTests=false -Dmaven.test.skip=false \
        "-Dmaven.repo.local=$maven_repository" \
        "-DpipestreamQuichePatchSha256=$(sha256sum "$transport_dir/quiche.patch" | cut -d ' ' -f 1)" \
        "-DpipestreamNettyPatchSha256=$(sha256sum "$transport_dir/netty.patch" | cut -d ' ' -f 1)" \
        verify
) 2>&1 | tee "$build_root/verify.log"

printf '\nVerified transport artifacts (not published or installed globally):\n'
find "$netty_source/codec-classes-quic/target" "$netty_source/codec-native-quic/target" \
    -maxdepth 1 -type f -name '*.jar' -exec sha256sum {} +
printf 'Full build and test outputs remain in %s\n' "$build_root"
