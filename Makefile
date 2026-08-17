.PHONY: fmt-check vet vuln test test-unit test-integration build migrate start-env stop-env

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

vet:
	GOWORK=off go vet ./...

vuln:
	GOWORK=off govulncheck ./...

test-unit:
	GOWORK=off go test -race ./...

test-integration:
	GOWORK=off go test -tags=integration -race ./test/integration/...

test: fmt-check vet test-unit

build:
	GOWORK=off go build -o bin/server ./cmd/server

migrate:
	@test -n "$(DATABASE_URL)" || (echo "DATABASE_URL required" && exit 1)
	@for migration in migrations/*.sql; do psql "$(DATABASE_URL)" -f "$$migration"; done

start-env:
	./scripts/start-env.sh

stop-env:
	./scripts/stop-env.sh
