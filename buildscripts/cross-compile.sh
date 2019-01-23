#!/bin/bash

# Enable tracing if set.
[ -n "$BASH_XTRACEFD" ] && set -ex

function _init() {
    ## All binaries are static make sure to disable CGO.
    export CGO_ENABLED=0

    ## List of architectures and OS to test coss compilation.
    SUPPORTED_OSARCH="linux/ppc64le linux/arm64 linux/s390x darwin/amd64 freebsd/amd64"
}

function _build_and_sign() {
    local osarch=$1
    IFS=/ read -r -a arr <<<"$osarch"
    os="${arr[0]}"
    arch="${arr[1]}"
    package=$(go list -f '{{.ImportPath}}')
    printf -- "--> %15s:%s\n" "${osarch}" "${package}"

    # Go build to build the binary.
    export GOOS=$os
    export GOARCH=$arch
    go build -tags kqueue -o /dev/null
}

function main() {
    echo "Testing builds for OS/Arch: ${SUPPORTED_OSARCH}"
    for each_osarch in ${SUPPORTED_OSARCH}; do
        _build_and_sign "${each_osarch}"
    done
}

_init && main "$@"
