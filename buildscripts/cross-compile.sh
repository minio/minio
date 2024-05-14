#!/bin/bash

HAS_CORRECT_GO=$(go version | grep "1.22")
CAN_BUILD_WITHOUT_DOCKER=${HAS_CORRECT_GO:-"false"}

set -e
# Enable tracing if set.
[ -n "$BASH_XTRACEFD" ] && set -x

function _init() {
    ## All binaries are static make sure to disable CGO.
    export CGO_ENABLED=0

    ## List of architectures and OS to test coss compilation.
    SUPPORTED_OSARCH="linux/arm64 darwin/arm64 darwin/amd64 linux/amd64"
}

function _build() {

    local osarch=$1
    IFS=/ read -r -a arr <<<"$osarch"
    os="${arr[0]}"
    arch="${arr[1]}"

    export GOOS=$os
    export GOARCH=$arch
    export GO111MODULE=on
    export CGO_ENABLED=0

    if [ "${CAN_BUILD_WITHOUT_DOCKER}" == "false" ]; then
        package="github.com/minio/minio"
    else
        package=$(go list -f '{{.ImportPath}}')
    fi

    printf -- "_build --> %15s:%s\n" "${osarch}" "${package}"

    # go build -trimpath to build the binary.
    if [ "${CAN_BUILD_WITHOUT_DOCKER}" == "false" ]; then
      docker run --name runner -d -v $PWD:$PWD --workdir $PWD golang:1.22 sleep infinity
      docker exec -t runner git config --global --add safe.directory $PWD
      docker exec -t runner go build -trimpath -tags kqueue --ldflags "${LDFLAGS}" -o "./bin/minio_${os}_${arch}" 1>/dev/null
      MUID=$(id -u)
      MGID=$(id -g)
      docker exec -t runner chown -R $MUID:$MGID ./bin
      docker stop runner
      docker rm runner
    else
      go build -trimpath -tags kqueue --ldflags "${LDFLAGS}" -o "./bin/minio_${os}_${arch}" 1>/dev/null
    fi

}

function _hash() {
  local osarch=$1
  IFS=/ read -r -a arr <<<"$osarch"
  os="${arr[0]}"
  arch="${arr[1]}"
  package="github.com/minio/minio"
  # package=$(go list -f '{{.ImportPath}}')
  printf -- "_hash --> %15s:%s\n" "${osarch}" "${package}"

  sha256sum "./bin/minio_${os}_${arch}" > "./bin/minio_${os}_${arch}".sha256sum
}

function _sign() {
  local osarch=$1
  IFS=/ read -r -a arr <<<"$osarch"
  os="${arr[0]}"
  arch="${arr[1]}"
  package="github.com/minio/minio"
  # package=$(go list -f '{{.ImportPath}}')
  printf -- "_sign --> %15s:%s\n" "${osarch}" "${package}"

  ./minisign -Sm "./bin/minio_${os}_${arch}" -W
  cp minisign.hash ./bin/$os/$arch/minisign.hash
}

function main() {
    echo "Get Minisign"

    wget "https://github.com/jedisct1/minisign/releases/download/0.11/minisign-0.11-linux.tar.gz"

    tar -zxvf minisign-0.11-linux.tar.gz

    mv minisign-linux/x86_64/minisign .

    echo "Create Signing Config:"
    ./minisign -G -f -p minisign.pub -s ~/.minisign/minisign.key -W  | grep "\-P" | awk '{print $5}' 1> minisign.hash

    echo "Testing builds for OS/Arch: ${SUPPORTED_OSARCH}"
    for each_osarch in ${SUPPORTED_OSARCH}; do
        _build "${each_osarch}"
        _hash "${each_osarch}"
        _sign "${each_osarch}"
    done
}

_init && main "$@"
