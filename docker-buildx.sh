#!/bin/bash

set -x

sudo sysctl net.ipv6.conf.wlp59s0.disable_ipv6=1

release=$(git describe --abbrev=0 --tags)
sudo docker buildx build --push --no-cache -t "minio/minio:latest" \
     --platform=linux/arm,linux/arm64,linux/amd64,linux/ppc64le,linux/s390x \
     -f Dockerfile.release .

sudo docker buildx build --push -t "minio/minio:${release}" \
     --platform=linux/arm,linux/arm64,linux/amd64,linux/ppc64le,linux/s390x \
     -f Dockerfile.release .

sudo sysctl net.ipv6.conf.wlp59s0.disable_ipv6=0

