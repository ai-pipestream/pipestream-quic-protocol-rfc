#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 OUTPUT_DIRECTORY" >&2
  exit 2
fi

output_directory=$1
mkdir -p "$output_directory"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj /CN=PipeStream-Conformance-CA \
  -addext basicConstraints=critical,CA:TRUE \
  -addext keyUsage=critical,keyCertSign,cRLSign \
  -keyout "$output_directory/ca.key" \
  -out "$output_directory/ca.crt" >/dev/null 2>&1

openssl req -new -newkey rsa:2048 -nodes \
  -subj /CN=localhost \
  -addext subjectAltName=DNS:localhost \
  -addext basicConstraints=critical,CA:FALSE \
  -addext extendedKeyUsage=serverAuth \
  -keyout "$output_directory/server.key" \
  -out "$output_directory/server.csr" >/dev/null 2>&1

openssl x509 -req -days 1 \
  -in "$output_directory/server.csr" \
  -CA "$output_directory/ca.crt" \
  -CAkey "$output_directory/ca.key" \
  -CAcreateserial \
  -copy_extensions copy \
  -out "$output_directory/server.crt" >/dev/null 2>&1
