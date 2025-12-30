set dotenv-load

app_name := "minio"

[private]
default: help

help:
    just --list --justfile {{justfile()}}

#########
# Build
#########

build:
    make build

install:
    make install

clean:
    make clean

#########
# Go
#########

tidy:
    go mod tidy

fmt:
    go fmt ./...

#########
# Testing
#########

test:
    make test

test-race:
    make test-race

verify:
    make verify

#########
# Linting
#########

lint:
    make lint

lint-fix:
    make lint-fix

#########
# Docker
#########

docker:
    make docker

docker-cicada:
    docker build -t {{app_name}}:latest -f Dockerfile.cicada .

#########
# Release Binaries
#########

# Build release binary for current platform
build-release:
    #!/usr/bin/env bash
    set -euxo pipefail
    mkdir -p ./out
    LDFLAGS=$(go run buildscripts/gen-ldflags.go)
    CGO_ENABLED=0 go build -tags kqueue -trimpath --ldflags "${LDFLAGS}" -o ./out/minio
    echo "Release binary built: ./out/minio"

# Build release binary for specific platform
build-release-platform GOOS GOARCH:
    #!/usr/bin/env bash
    set -euxo pipefail
    mkdir -p ./out
    LDFLAGS=$(go run buildscripts/gen-ldflags.go)
    CGO_ENABLED=0 GOOS={{GOOS}} GOARCH={{GOARCH}} go build -tags kqueue -trimpath --ldflags "${LDFLAGS}" -o ./out/minio-{{GOOS}}-{{GOARCH}}
    echo "Release binary built: ./out/minio-{{GOOS}}-{{GOARCH}}"

# Build release binaries for x64 and arm64
build-release-all:
    just build-release-platform linux amd64
    just build-release-platform linux arm64
    echo "All release binaries built in ./out/"
