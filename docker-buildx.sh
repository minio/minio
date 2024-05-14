#!/bin/bash

release=$(git describe --abbrev=0 --tags)
GIT_HASH=$(git rev-parse --short=8 HEAD)
release="${release}-${GIT_HASH}"

docker buildx build --push --no-cache \
       --build-arg RELEASE="${release}" -t "quay.io/stackstate/minio:${release}" \
       --platform=linux/arm64,linux/amd64 \
       -f Dockerfile.release .

docker buildx prune -f