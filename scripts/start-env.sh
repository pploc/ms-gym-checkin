#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION="${AWS_REGION:-us-east-1}"
export KMS_KEY_ID="${KMS_KEY_ID:-alias/checkin-root}"
export KMS_ENDPOINT_URL="${KMS_ENDPOINT_URL:-http://127.0.0.1:4566}"
export DATABASE_URL="${DATABASE_URL:-postgres://yugabyte@127.0.0.1:5434/checkin_db?sslmode=disable}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
export SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://127.0.0.1:8081}"

command -v aws >/dev/null || { printf 'aws is required\n' >&2; exit 1; }
command -v curl >/dev/null || { printf 'curl is required\n' >&2; exit 1; }
command -v docker >/dev/null || { printf 'docker is required\n' >&2; exit 1; }

cd "$root"
docker compose up -d yugabyte localstack kafka schema-registry kafka-unseeded schema-registry-unseeded

for _ in {1..60}; do
  if docker compose exec -T yugabyte sh -c 'ysqlsh -h "$(hostname)" -c "SELECT 1"' >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker compose exec -T yugabyte sh -c 'ysqlsh -h "$(hostname)" -c "SELECT 1"' >/dev/null
docker compose exec -T yugabyte sh -c 'ysqlsh -h "$(hostname)" -tAc "SELECT 1 FROM pg_database WHERE datname = '\''checkin_db'\''" | grep -qx 1 || ysqlsh -h "$(hostname)" -c "CREATE DATABASE checkin_db"' >/dev/null

for _ in {1..60}; do
  if curl --fail --silent "$KMS_ENDPOINT_URL/_localstack/health" >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent "$KMS_ENDPOINT_URL/_localstack/health" >/dev/null

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

aws --endpoint-url "$KMS_ENDPOINT_URL" kms describe-key --key-id "$KMS_KEY_ID" >/dev/null 2>&1 || {
  key_id=$(aws --endpoint-url "$KMS_ENDPOINT_URL" kms create-key --description 'ms-gym-checkin local QR root key' --query 'KeyMetadata.KeyId' --output text)
  aws --endpoint-url "$KMS_ENDPOINT_URL" kms create-alias --alias-name "$KMS_KEY_ID" --target-key-id "$key_id" >/dev/null
}

printf 'local dependencies ready\n'
printf 'export DATABASE_URL=%q AWS_REGION=%q KMS_KEY_ID=%q KMS_ENDPOINT_URL=%q KAFKA_BROKERS=%q SCHEMA_REGISTRY_URL=%q UNSEEDED_SCHEMA_REGISTRY_URL=%q\n' \
  "$DATABASE_URL" "$AWS_REGION" "$KMS_KEY_ID" "$KMS_ENDPOINT_URL" "$KAFKA_BROKERS" "$SCHEMA_REGISTRY_URL" 'http://127.0.0.1:8082'
