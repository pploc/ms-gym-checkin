.PHONY: fmt-check vet test test-unit test-integration build migrate

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

vet:
	GOWORK=off go vet ./...

test-unit:
	GOWORK=off go test -race ./...

test-integration:
	GOWORK=off go test -tags=integration -race ./test/integration/...

test: fmt-check vet test-unit

build:
	GOWORK=off go build -o bin/server ./cmd/server

migrate:
	@test -n "$(DATABASE_URL)" || (echo "DATABASE_URL required" && exit 1)
	psql "$(DATABASE_URL)" -f migrations/001_init.sql
