#!/bin/bash

sudo sysctl net.ipv6.conf.all.disable_ipv6=0

release=$(git describe --abbrev=0 --tags)

alpine_version="3.18.2"

docker buildx build --push --no-cache \
	--build-arg RELEASE="${release}" \
	-t "minio/minio:${release}.alpine-${alpine_version}" \
	-t "quay.io/minio/minio:${release}.alpine-${alpine_version}" \
	--platform=linux/amd64 -f Dockerfile.alpine.cicd .

docker buildx prune -f

sudo sysctl net.ipv6.conf.all.disable_ipv6=0
