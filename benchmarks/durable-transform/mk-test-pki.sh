#!/usr/bin/env bash
# Test-only loopback PKI for the durable-transform comparison.
# Creates a throwaway CA, three authority server certs, one coordinator
# client cert, and principal maps. NEVER production credentials.
set -euo pipefail

ROOT="${1:-./test-pki}"
mkdir -p "$ROOT"

ca() {
  local name="$1"
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj "/CN=$name-ca" -keyout "$ROOT/$name-ca.key" -out "$ROOT/$name-ca.pem" 2>/dev/null
}

leaf() { # leaf <ca> <cn> <out> [client|server]
  local ca="$1" cn="$2" out="$3" kind="${4:-server}"
  local ext="$ROOT/ext.cnf"
  if [ "$kind" = client ]; then
    printf 'basicConstraints=CA:FALSE\nextendedKeyUsage=clientAuth\nsubjectAltName=DNS:%s\n' "$cn" > "$ext"
  else
    printf 'basicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:%s,IP:127.0.0.1\n' "$cn" > "$ext"
  fi
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$cn" \
    -keyout "$ROOT/$out.key" -out "$ROOT/$out.csr" 2>/dev/null
  openssl x509 -req -in "$ROOT/$out.csr" -CA "$ROOT/$ca-ca.pem" -CAkey "$ROOT/$ca-ca.key" \
    -CAcreateserial -days 2 -extfile "$ext" -out "$ROOT/$out.pem" 2>/dev/null
  rm -f "$ROOT/$out.csr" "$ROOT/ext.cnf" "$ROOT/$ca-ca.srl"
}

fingerprint() { # fingerprint <pem> -> lowercase hex sha256 of leaf DER
  openssl x509 -in "$1" -outform DER 2>/dev/null | openssl dgst -sha256 | awk '{print $NF}'
}

ca ps
ca grpc

for w in a b c; do
  leaf ps "localhost" "ps-server-$w" server
  leaf grpc "localhost" "grpc-server-$w" server
done
leaf ps "metacoord" "ps-client" client
leaf grpc "metacoord" "grpc-client" client

{
  echo -e "sha256\tprincipal"
  echo -e "$(fingerprint "$ROOT/ps-client.pem")\tworkload"
} > "$ROOT/ps-principals.tsv"
{
  echo -e "sha256\tprincipal"
  echo -e "$(fingerprint "$ROOT/grpc-client.pem")\tworkload"
} > "$ROOT/grpc-principals.tsv"

echo "PKI ready in $ROOT (valid ~2 days, loopback only)"
