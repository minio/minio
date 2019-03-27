#!/usr/bin/env bash

set -e


CGO_ENABLED=0 go test -v -coverprofile=coverage.txt -covermode=atomic ./...
