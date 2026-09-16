#!/usr/bin/env bash
# Test-only loopback PKI for the durable-transform comparison.
# Creates a throwaway CA, three authority server certs, one coordinator
# client cert, and principal maps. NEVER production credentials.
# Env (all optional, defaults preserve historical behavior; every use is
# recorded by the invoking suite script):
#   PKI_DAYS=N          validity days for standard leaves (default 2)
#   PKI_SHORT_SECONDS=N issue the coordinator client leaf (ps-client,
#                       grpc-client) with notBefore=now, notAfter=now+N
#                       via python cryptography (openssl x509 -req has no
#                       seconds precision); seconds precision is required
#                       by the A6 expiry rows
#   PKI_SECOND_LEAF=1   extra metacoord client leaves (ps-client2,
#                       grpc-client2) + TSV rows mapping fp2 to workload
#                       (A6 rotation row)
#   PKI_INTRUDER=1      intruder client leaves (ps-intruder, grpc-intruder)
#                       + TSV rows mapping fp to intruder (A6 foreign-owner)
set -euo pipefail

ROOT="${1:-./test-pki}"
mkdir -p "$ROOT"

ca() {
  local name="$1"
  openssl req -x509 -newkey rsa:2048 -nodes -days "${PKI_DAYS:-2}" \
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
    -CAcreateserial -days "${PKI_DAYS:-2}" -extfile "$ext" -out "$ROOT/$out.pem" 2>/dev/null
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

# TEST-ONLY short-lived coordinator leaf (A6 expiry): seconds precision via
# python cryptography, signed by the same throwaway CA key.
if [ -n "${PKI_SHORT_SECONDS:-}" ]; then
  python3 - "$ROOT" "$PKI_SHORT_SECONDS" <<'PYEOF'
import datetime, sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
root, seconds = sys.argv[1], int(sys.argv[2])
now = datetime.datetime.now(datetime.timezone.utc)
for ca, prefix in (("ps", "ps-client"), ("grpc", "grpc-client")):
    with open(f"{root}/{ca}-ca.pem", "rb") as f:
        ca_cert = x509.load_pem_x509_certificate(f.read())
    with open(f"{root}/{ca}-ca.key", "rb") as f:
        ca_key = serialization.load_pem_private_key(f.read(), password=None)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "metacoord")]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(seconds=seconds))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("metacoord")]), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )
    with open(f"{root}/{prefix}.key", "wb") as f:
        f.write(key.private_bytes(serialization.Encoding.PEM,
              serialization.PrivateFormat.TraditionalOpenSSL,
              serialization.NoEncryption()))
    with open(f"{root}/{prefix}.pem", "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
print(f"short-lived coordinator leaves issued ({seconds}s)")
PYEOF
  # Refresh the TSV rows: fingerprints changed.
  {
    echo -e "sha256\tprincipal"
    echo -e "$(fingerprint "$ROOT/ps-client.pem")\tworkload"
  } > "$ROOT/ps-principals.tsv"
  {
    echo -e "sha256\tprincipal"
    echo -e "$(fingerprint "$ROOT/grpc-client.pem")\tworkload"
  } > "$ROOT/grpc-principals.tsv"
fi

# TEST-ONLY second metacoord leaf (A6 rotation): same CN, new key.
if [ "${PKI_SECOND_LEAF:-0}" = 1 ]; then
  leaf ps "metacoord" "ps-client2" client
  leaf grpc "metacoord" "grpc-client2" client
  echo -e "$(fingerprint "$ROOT/ps-client2.pem")\tworkload" >> "$ROOT/ps-principals.tsv"
  echo -e "$(fingerprint "$ROOT/grpc-client2.pem")\tworkload" >> "$ROOT/grpc-principals.tsv"
fi

# TEST-ONLY intruder principal (A6 foreign owner).
if [ "${PKI_INTRUDER:-0}" = 1 ]; then
  leaf ps "intruder" "ps-intruder" client
  leaf grpc "intruder" "grpc-intruder" client
  echo -e "$(fingerprint "$ROOT/ps-intruder.pem")\tintruder" >> "$ROOT/ps-principals.tsv"
  echo -e "$(fingerprint "$ROOT/grpc-intruder.pem")\tintruder" >> "$ROOT/grpc-principals.tsv"
fi

echo "PKI ready in $ROOT (valid ~${PKI_DAYS:-2} days, loopback only)"
