#!/usr/bin/env bash

set -e

for d in $(go list ./... | grep -v browser); do
    CGO_ENABLED=1 go test -v -race --timeout 50m "$d"
done
