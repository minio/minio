#!/bin/bash

sudo sysctl net.ipv6.conf.wlp59s0.disable_ipv6=1

release=$(git describe --abbrev=0 --tags)

docker buildx build --push --no-cache \
       --build-arg RELEASE="${release}" -t "minio/minio:latest" \
       --platform=linux/arm64,linux/amd64,linux/ppc64le,linux/s390x \
       -f Dockerfile.release .

docker buildx prune -f

docker buildx build --push --no-cache \
       --build-arg RELEASE="${release}" -t "minio/minio:${release}" \
       --platform=linux/arm64,linux/amd64,linux/ppc64le,linux/s390x \
       -f Dockerfile.release .

docker buildx prune -f

docker buildx build --push --no-cache \
       --build-arg RELEASE="${release}" -t "quay.io/minio/minio:${release}" \
       --platform=linux/arm64,linux/amd64,linux/ppc64le,linux/s390x \
       -f Dockerfile.release .

docker buildx prune -f

docker buildx build --push --no-cache \
       --build-arg RELEASE="${release}" -t "minio/minio:${release}.fips" \
       --platform=linux/amd64 -f Dockerfile.release.fips .

docker buildx prune -f

docker buildx build --push --no-cache \
       --build-arg RELEASE="${release}" -t "quay.io/minio/minio:${release}.fips" \
       --platform=linux/amd64 -f Dockerfile.release.fips .

docker buildx prune -f

sudo sysctl net.ipv6.conf.wlp59s0.disable_ipv6=0
