#!/usr/bin/env bash

set -e

for d in $(go list ./...); do
    CGO_ENABLED=1 go test -v -tags kqueue -race --timeout 100m "$d"
done
