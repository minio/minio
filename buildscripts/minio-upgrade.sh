#!/bin/bash

trap 'cleanup $LINENO' ERR

# shellcheck disable=SC2120
cleanup() {
    MINIO_VERSION=dev docker-compose \
                 -f "buildscripts/upgrade-tests/compose.yml" \
                 rm -s -f
    docker volume prune -f
}

__init__() {
    sudo apt install curl -y
    export GOPATH=/tmp/gopath
    export PATH=${PATH}:${GOPATH}/bin

    go install github.com/minio/mc@latest

    TAG=minio/minio:dev make docker

    MINIO_VERSION=RELEASE.2019-12-19T22-52-26Z docker-compose \
                 -f "buildscripts/upgrade-tests/compose.yml" \
                 up -d --build
    until (mc alias set minio http://127.0.0.1:9000 minioadmin minioadmin); do
        echo "...waiting..." && sleep 5;
    done

    mc mb minio/minio-test/
    mc cp ./minio minio/minio-test/to-read/
    mc cp /etc/hosts minio/minio-test/to-read/hosts
    mc policy set download minio/minio-test
    mc cat minio/minio-test/to-read/minio | sha256sum
    mc cat ./minio | sha256sum
    curl -s http://127.0.0.1:9000/minio-test/to-read/hosts | sha256sum

    MINIO_VERSION=dev docker-compose -f "buildscripts/upgrade-tests/compose.yml" stop
}

verify_checksum_after_heal() {
    sum1=$(curl -s "$2" | sha256sum);
    mc admin heal --json -r "$1" >/dev/null; # test after healing
    sum1_heal=$(curl -s "$2" | sha256sum);

    if [ "${sum1_heal}" != "${sum1}" ]; then
        echo "mismatch expected ${sum1_heal}, got ${sum1}"
        exit 1;
    fi
}

verify_checksum_mc() {
    expected=$(mc cat "$1" | sha256sum)
    got=$(mc cat "$2" | sha256sum)

    if [ "${expected}" != "${got}" ]; then
        echo "mismatch expected ${expected}, got ${got}"
        exit 1;
    fi
}

main() {
    MINIO_VERSION=dev docker-compose -f "buildscripts/upgrade-tests/compose.yml" up -d --build

    until (mc alias set minio http://127.0.0.1:9000 minioadmin minioadmin); do
        echo "...waiting..." && sleep 5
    done

    verify_checksum_after_heal minio/minio-test http://127.0.0.1:9000/minio-test/to-read/hosts

    verify_checksum_mc ./minio minio/minio-test/to-read/minio

    verify_checksum_mc /etc/hosts minio/minio-test/to-read/hosts

    cleanup
}

( __init__ "$@" && main "$@" )
