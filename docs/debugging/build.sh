#!/bin/bash

export CGO_ENABLED=0
for dir in docs/debugging/*/; do
	go build -C ${dir} -v
done
