#!/bin/bash -e
#
#  Mint (C) 2017 Minio, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

MINIO_GO_VERSION=$(curl --retry 10 -Ls -o /dev/null -w "%{url_effective}" https://github.com/minio/minio-go/releases/latest | sed "s/https:\/\/github.com\/minio\/minio-go\/releases\/tag\///")
if [ -z "$MINIO_GO_VERSION" ]; then
    echo "unable to get minio-go version from github"
    exit 1
fi

test_run_dir="$MINT_RUN_CORE_DIR/minio-go"
curl -sL -o "${test_run_dir}/main.go" "https://raw.githubusercontent.com/minio/minio-go/${MINIO_GO_VERSION}/functional_tests.go"
(cd "$test_run_dir" && GO111MODULE=on CGO_ENABLED=0 go build -o minio-go main.go)
