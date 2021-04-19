#!/bin/bash -e
#
#

MINIO_GO_VERSION=$(curl --retry 10 -Ls -o /dev/null -w "%{url_effective}" https://github.com/minio/minio-go/releases/latest | sed "s/https:\/\/github.com\/minio\/minio-go\/releases\/tag\///")
if [ -z "$MINIO_GO_VERSION" ]; then
    echo "unable to get minio-go version from github"
    exit 1
fi

test_run_dir="$MINT_RUN_CORE_DIR/minio-go"
curl -sL -o "${test_run_dir}/main.go" "https://raw.githubusercontent.com/minio/minio-go/${MINIO_GO_VERSION}/functional_tests.go"
(cd "$test_run_dir" && GO111MODULE=on CGO_ENABLED=0 go build -o minio-go main.go)
