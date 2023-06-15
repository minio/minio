#!/usr/bin/env bash

set -e

export GORACE="history_size=7"
export MINIO_API_REQUESTS_MAX=10000

for d in $(go list ./...); do
	CGO_ENABLED=1 go test -v -race --timeout 100m "$d"
done
