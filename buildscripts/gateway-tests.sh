#!/bin/bash
#
# MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

function start_minio_server()
{
    MINIO_ACCESS_KEY=minio MINIO_SECRET_KEY=minio123 \
                    minio --quiet --json server /data --address 127.0.0.1:24242 > server.log 2>&1 &
    server_pid=$!
    sleep 10

    echo "$server_pid"
}

function start_minio_gateway_s3()
{
    MINIO_ACCESS_KEY=minio MINIO_SECRET_KEY=minio123 \
                    minio --quiet --json gateway s3 http://127.0.0.1:24242 \
                    --address 127.0.0.1:24240 > gateway.log 2>&1 &
    gw_pid=$!
    sleep 10

    echo "$gw_pid"
}

function main()
{
    sr_pid="$(start_minio_server)"
    gw_pid="$(start_minio_gateway_s3)"

    SERVER_ENDPOINT=127.0.0.1:24240 ENABLE_HTTPS=0 ACCESS_KEY=minio \
                   SECRET_KEY=minio123 MINT_MODE="full" /mint/entrypoint.sh \
                   aws-sdk-go aws-sdk-java aws-sdk-php aws-sdk-ruby awscli \
                   healthcheck mc minio-dotnet minio-js \
                   minio-py s3cmd s3select security
    rv=$?

    kill "$sr_pid"
    kill "$gw_pid"
    sleep 3

    if [ "$rv" -ne 0 ]; then
        echo "=========== Gateway ==========="
        cat "gateway.log"
        echo "=========== Server ==========="
        cat "server.log"
    fi

    rm -f gateway.log server.log
}

main "$@"
