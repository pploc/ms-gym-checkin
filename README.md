# ms-gym-checkin

Check-in owns encrypted per-gym QR root keys, signed display payloads, idempotent check-in records, and `checkin.recorded.v1` outbox publication.

## Scope

- Native mTLS gRPC on `:50051`; public operations receive trusted claims only from generated gateway identity.
- `:8080` serves only `/healthz` and `/readyz`.
- Member validation uses Check-in-only `ValidateMembership(user_id, gym_id)` mTLS RPC.
- Display/key administration uses Check-in-only Plans `ValidateCheckInGym(gym_id)` mTLS RPC.
- No kiosk/device lifecycle, Redis, public native business HTTP, copied Member/Plans state, or cross-service foreign keys.

## Immutable dependencies

- `github.com/pploc/proto-go v1.7.1`
- `github.com/pploc/common-go v0.5.0-rc.1`

Use `GOWORK=off`. Do not add `replace` directives, sibling checkout dependencies, or mutable branch references.

## QR contract

```text
v1.<gym_id>.<key_version>.<utc_slot>.<mac_base64url>
message = checkin-qr:v1|<gym_id>|<key_version>|<utc_slot>
```

Gym IDs must be canonical lowercase UUID text. Root keys are 32 random bytes encrypted at rest with AWS KMS. KMS encrypts root-key bytes only; QR signing remains HMAC-SHA-256. Plaintext keys, KMS ciphertext, JWTs, credentials, raw QR payloads, and request bodies must never reach logs, events, evidence, or committed files.

## Required configuration

```text
DATABASE_URL
AWS_REGION
KMS_KEY_ID
MEMBER_GRPC_ADDR / MEMBER_GRPC_CERT / MEMBER_GRPC_KEY / MEMBER_GRPC_CA
PLANS_GRPC_ADDR / PLANS_GRPC_CERT / PLANS_GRPC_KEY / PLANS_GRPC_CA
CHECKIN_GRPC_SERVER_CERT / CHECKIN_GRPC_SERVER_KEY / CHECKIN_GRPC_CLIENT_CA
KAFKA_BROKERS
SCHEMA_REGISTRY_URL
```

Production uses AWS SDK default credentials through EKS workload identity. Grant only `kms:Encrypt`, `kms:Decrypt`, and `kms:DescribeKey` on Check-in's CMK. Do not configure static AWS credentials. `KMS_KEY_ID` may be an alias or ARN; stored rows retain KMS's resolved key ARN.

Optional: `GRPC_ADDR`, `HTTP_ADDR`, `KMS_ENDPOINT_URL` (LocalStack only), `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `OUTBOX_RELAY_INTERVAL`, `SHUTDOWN_TIMEOUT`, `READINESS_TIMEOUT`, `GRPC_REFLECTION`. Reflection defaults to disabled and must remain disabled outside isolated internal development.

## Local verification

```bash
GOWORK=off go mod download
make test
make build
govulncheck ./...
```

Start local Yugabyte, LocalStack KMS, Kafka, and Schema Registry:

```bash
make start-env
export DATABASE_URL='postgres://yugabyte@127.0.0.1:5434/checkin_db?sslmode=disable'
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
export KMS_KEY_ID='alias/checkin-root'
export KMS_ENDPOINT_URL='http://127.0.0.1:4566'
export KAFKA_BROKERS='127.0.0.1:9092'
export SCHEMA_REGISTRY_URL='http://127.0.0.1:8081'
export UNSEEDED_SCHEMA_REGISTRY_URL='http://127.0.0.1:8082'
make migrate
```

`make start-env` provisions local database, KMS key, topic, seeded-path Registry at `:8081`, and separate empty lookup-only proof Registry at `:8082`. It does not register schemas. For disposable local Kafka verification, seed only empty `:8081` Registry:

```bash
cd ../gym-proto
./gradlew seedConfluentSchemas -PschemaRegistryUrl=http://127.0.0.1:8081 --no-daemon
cd ../ms-gym-checkin
```

This controlled seed is only local registration exception. Runtime remains lookup-only. `generateConfluentFixtures` requires a clean Registry and must not run here. Stop all local state with `make stop-env`.

Migrations are explicit. Application startup never mutates schema. `002_kms_cutover.sql` refuses a non-empty legacy root-key table because KMS cannot decrypt prior Transit ciphertext. Production deployment requires the table to be empty before the KMS-only cutover.

Integration checks require live Yugabyte, LocalStack KMS, Kafka, and Schema Registry:

```bash
make test-integration
```

## Delivery

Docker build runs non-root. Private module credentials are accepted only through BuildKit secret `github_token`; never use build arguments. Image publication is manual `develop` dispatch and must use immutable source-SHA tags only.
