#!/usr/bin/env bash

set -e

counter=1
for d in $(go list ./... | grep -v browser); do
    CGO_ENABLED=1 go test -v -race --timeout 50m "$d" -coverprofile=./coverage-$counter.txt -covermode=atomic
    ((counter++))
done
