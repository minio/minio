#!/usr/bin/env bash

set -e

GOPROXY=https://proxy.golang.org GO111MODULE=on CGO_ENABLED=0 go test -v -coverprofile=coverage.txt -covermode=atomic ./...
