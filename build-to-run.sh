#!/bin/bash

echo "get gen-ldflags ..."
s=$(go run buildscripts/gen-ldflags.go)
echo "build minio ..."
go build -tags kqueue -trimpath --ldflags "${s}"
echo "success"
