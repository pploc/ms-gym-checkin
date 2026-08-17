#!/usr/bin/env bash
set -euo pipefail

out=${1:-"$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.tmp/test-certs"}
rm -rf "$out"
mkdir -p "$out"
trap 'rm -f "$out"/*.csr "$out"/*.ext "$out"/*.srl' EXIT

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$out/ca.key" -out "$out/ca.crt" -subj '/CN=checkin-test-ca' >/dev/null 2>&1

issue() {
  local name=$1 san=$2 usage=$3
  openssl req -newkey rsa:2048 -nodes -keyout "$out/$name.key" -out "$out/$name.csr" -subj "/CN=$name" >/dev/null 2>&1
  printf 'subjectAltName=%s\nextendedKeyUsage=%s\nbasicConstraints=CA:FALSE\n' "$san" "$usage" >"$out/$name.ext"
  openssl x509 -req -days 1 -in "$out/$name.csr" -CA "$out/ca.crt" -CAkey "$out/ca.key" -CAcreateserial -out "$out/$name.crt" -extfile "$out/$name.ext" >/dev/null 2>&1
}

issue server 'DNS:localhost,IP:127.0.0.1' serverAuth
issue client-checkin 'DNS:ms-gym-checkin,URI:spiffe://gym.cluster.local/ns/gym-system/sa/ms-gym-checkin' clientAuth
issue client-other 'DNS:not-checkin' clientAuth
chmod 600 "$out"/*.key
printf '%s\n' "$out"
