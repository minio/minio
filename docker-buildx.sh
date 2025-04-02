#!/bin/bash

sudo sysctl net.ipv6.conf.all.disable_ipv6=0

remote=$(git remote get-url upstream)
if test "$remote" != "git@github.com:minio/minio.git"; then
	echo "Script requires that the 'upstream' remote is set to git@github.com:minio/minio.git"
	exit 1
fi

git remote update upstream && git checkout master && git rebase upstream/master

release=$(git describe --abbrev=0 --tags)

docker buildx build --push --no-cache \
	--build-arg RELEASE="${release}" \
	-t "minio/minio:latest" \
	-t "minio/minio:latest-cicd" \
	-t "quay.io/minio/minio:latest" \
	-t "quay.io/minio/minio:latest-cicd" \
	-t "minio/minio:${release}" \
	-t "quay.io/minio/minio:${release}" \
	--platform=linux/arm64,linux/amd64,linux/ppc64le \
	-f Dockerfile.release .

docker buildx prune -f

docker buildx build --push --no-cache \
	--build-arg RELEASE="${release}" \
	-t "minio/minio:${release}-cpuv1" \
	-t "quay.io/minio/minio:${release}-cpuv1" \
	--platform=linux/arm64,linux/amd64,linux/ppc64le \
	-f Dockerfile.release.old_cpu .

docker buildx prune -f

sudo sysctl net.ipv6.conf.all.disable_ipv6=0
