#!/bin/bash

set -ex

function _init() {
	## All binaries are static make sure to disable CGO.
	export CGO_ENABLED=0
	export CRED_DIR="/media/${USER}/minio"

	## List of architectures and OS to test coss compilation.
	SUPPORTED_OSARCH="linux/ppc64le linux/amd64 linux/arm64"

	remote=$(git remote get-url upstream)
	if test "$remote" != "git@github.com:minio/minio.git"; then
		echo "Script requires that the 'upstream' remote is set to git@github.com:minio/minio.git"
		exit 1
	fi

	git remote update upstream && git checkout master && git rebase upstream/master

	release=$(git describe --abbrev=0 --tags)
	export release
}

function _build() {
	local osarch=$1
	IFS=/ read -r -a arr <<<"$osarch"
	os="${arr[0]}"
	arch="${arr[1]}"
	package=$(go list -f '{{.ImportPath}}')
	printf -- "--> %15s:%s\n" "${osarch}" "${package}"

	# go build -trimpath to build the binary.
	export GOOS=$os
	export GOARCH=$arch
	export MINIO_RELEASE=RELEASE
	LDFLAGS=$(go run buildscripts/gen-ldflags.go)
	go build -tags kqueue -trimpath --ldflags "${LDFLAGS}" -o ./minio-${arch}.${release}
	minisign -qQSm ./minio-${arch}.${release} -s "$CRED_DIR/minisign.key" <"$CRED_DIR/minisign-passphrase"

	sha256sum_str=$(sha256sum <./minio-${arch}.${release})
	rc=$?
	if [ "$rc" -ne 0 ]; then
		abort "unable to generate sha256sum for ${1}"
	fi
	echo "${sha256sum_str// -/minio.${release}}" >./minio-${arch}.${release}.sha256sum
}

function main() {
	echo "Testing builds for OS/Arch: ${SUPPORTED_OSARCH}"
	for each_osarch in ${SUPPORTED_OSARCH}; do
		_build "${each_osarch}"
	done

	sudo sysctl net.ipv6.conf.all.disable_ipv6=0

	docker buildx build --push --no-cache \
		--build-arg RELEASE="${release}" \
		-t "registry.min.dev/community/minio:latest" \
		-t "registry.min.dev/community/minio:${release}" \
		--platform=linux/arm64,linux/amd64,linux/ppc64le \
		-f Dockerfile .

	docker buildx prune -f

	sudo sysctl net.ipv6.conf.all.disable_ipv6=0
}

_init && main "$@"
