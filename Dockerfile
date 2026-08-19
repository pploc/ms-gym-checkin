# syntax=docker/dockerfile:1
FROM golang:1.26.6-bookworm AS build
WORKDIR /src
ENV GOPRIVATE=github.com/pploc/*
COPY go.mod go.sum ./
RUN --mount=type=secret,id=github_token,required=true \
    sh -ec 'home=$(mktemp -d); export HOME="$home"; git config --global url."https://x-access-token:$(cat /run/secrets/github_token)@github.com/".insteadOf "https://github.com/"; GOWORK=off go mod download; rm -rf "$home"'
COPY . .
RUN CGO_ENABLED=0 GOWORK=off go build -trimpath -ldflags='-s -w' -o /out/server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/server /server
USER nonroot:nonroot
EXPOSE 50051 8080
ENTRYPOINT ["/server"]
