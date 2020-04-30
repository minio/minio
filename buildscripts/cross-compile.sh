#!/bin/bash

set -e
# Enable tracing if set.
[ -n "$BASH_XTRACEFD" ] && set -x

function _init() {
    ## All binaries are static make sure to disable CGO.
    export CGO_ENABLED=0

    ## List of architectures and OS to test coss compilation.
    SUPPORTED_OSARCH="linux/ppc64le linux/arm64 linux/s390x darwin/amd64 freebsd/amd64 windows/amd64 linux/arm linux/386"
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
    export GO111MODULE=on
    go build -trimpath -tags kqueue -o /dev/null
}

function main() {
    echo "Testing builds for OS/Arch: ${SUPPORTED_OSARCH}"
    for each_osarch in ${SUPPORTED_OSARCH}; do
        _build "${each_osarch}"
    done
}

_init && main "$@"
