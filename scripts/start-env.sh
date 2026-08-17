#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ -z "${VAULT_DEV_ROOT_TOKEN_ID:-}" ]]; then
  printf 'VAULT_DEV_ROOT_TOKEN_ID is required\n' >&2
  exit 1
fi
export VAULT_ADDR="${VAULT_ADDR:-http://127.0.0.1:8200}"
export DATABASE_URL="${DATABASE_URL:-postgres://yugabyte@127.0.0.1:5434/checkin_db?sslmode=disable}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
export SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://127.0.0.1:8081}"

command -v docker >/dev/null || { printf 'docker is required\n' >&2; exit 1; }
command -v curl >/dev/null || { printf 'curl is required\n' >&2; exit 1; }

cd "$root"
docker compose up -d yugabyte vault kafka schema-registry kafka-unseeded schema-registry-unseeded

for _ in {1..60}; do
  if docker compose exec -T yugabyte sh -c 'ysqlsh -h "$(hostname)" -c "SELECT 1"' >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker compose exec -T yugabyte sh -c 'ysqlsh -h "$(hostname)" -c "SELECT 1"' >/dev/null
docker compose exec -T yugabyte sh -c 'ysqlsh -h "$(hostname)" -tAc "SELECT 1 FROM pg_database WHERE datname = '\''checkin_db'\''" | grep -qx 1 || ysqlsh -h "$(hostname)" -c "CREATE DATABASE checkin_db"' >/dev/null

for _ in {1..60}; do
  if curl --fail --silent "$VAULT_ADDR/v1/sys/health" >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent "$VAULT_ADDR/v1/sys/health" >/dev/null

for _ in {1..60}; do
  if curl --fail --silent "$SCHEMA_REGISTRY_URL/subjects" >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent "$SCHEMA_REGISTRY_URL/subjects" >/dev/null

for _ in {1..60}; do
  if curl --fail --silent http://127.0.0.1:8082/subjects >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent http://127.0.0.1:8082/subjects >/dev/null

docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic checkin.recorded.v1 --partitions 1 --replication-factor 1 >/dev/null

curl --fail --silent --header "X-Vault-Token: $VAULT_DEV_ROOT_TOKEN_ID" --request POST \
  --data '{"type":"transit"}' "$VAULT_ADDR/v1/sys/mounts/transit" >/dev/null || true
curl --fail --silent --header "X-Vault-Token: $VAULT_DEV_ROOT_TOKEN_ID" --request POST \
  --data '{"type":"aes256-gcm96"}' "$VAULT_ADDR/v1/transit/keys/checkin-root" >/dev/null || true

printf 'local dependencies ready\n'
printf 'export DATABASE_URL=%q VAULT_ADDR=%q KAFKA_BROKERS=%q SCHEMA_REGISTRY_URL=%q UNSEEDED_SCHEMA_REGISTRY_URL=%q\n' \
  "$DATABASE_URL" "$VAULT_ADDR" "$KAFKA_BROKERS" "$SCHEMA_REGISTRY_URL" 'http://127.0.0.1:8082'
