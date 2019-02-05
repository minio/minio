#!/bin/bash
#
# Minio Cloud Storage, (C) 2019 Minio, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
set -E
set -o pipefail

export SERVER_ENDPOINT=127.0.0.1:24240
export ENABLE_HTTPS=0
export ACCESS_KEY=minio
export SECRET_KEY=minio123
export MINIO_ACCESS_KEY=minio
export MINIO_SECRET_KEY=minio123
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio123

trap "cat server.log;cat gateway.log" SIGHUP SIGINT SIGTERM

./minio --quiet --json server data --address 127.0.0.1:24242 > server.log &
sleep 3
./minio --quiet --json gateway s3 http://127.0.0.1:24242 --address 127.0.0.1:24240 > gateway.log &
sleep 3

mkdir -p /mint
git clone https://github.com/minio/mint /mint
cd /mint

export MINT_ROOT_DIR=${MINT_ROOT_DIR:-/mint}
export MINT_RUN_CORE_DIR="$MINT_ROOT_DIR/run/core"
export MINT_RUN_SECURITY_DIR="$MINT_ROOT_DIR/run/security"
export WGET="wget --quiet --no-check-certificate"

go get github.com/go-ini/ini

./create-data-files.sh
./preinstall.sh

# install mint app packages
for pkg in "build"/*/install.sh; do
    echo "Running $pkg"
    $pkg
done

./postinstall.sh

/mint/entrypoint.sh || cat server.log gateway.log fail
