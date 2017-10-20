#!/bin/bash
#
# Minio Cloud Storage, (C) 2017 Minio, Inc.
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

if [ ! -x "$PWD/minio" ]; then
    echo "minio executable binary not found in current directory"
    exit 1
fi

WORK_DIR="$PWD/.verify-$RANDOM"

export MINT_MODE=core
export MINT_DATA_DIR="$WORK_DIR/data"
export SERVER_ENDPOINT="127.0.0.1:9000"
export ACCESS_KEY="minio"
export SECRET_KEY="minio123"
export ENABLE_HTTPS=0

MINIO_CONFIG_DIR="$WORK_DIR/.minio"
MINIO=( "$PWD/minio" --config-dir "$MINIO_CONFIG_DIR" )

FILE_1_MB="$MINT_DATA_DIR/datafile-1-MB"
FILE_65_MB="$MINT_DATA_DIR/datafile-65-MB"

FUNCTIONAL_TESTS="$WORK_DIR/functional-tests.sh"

function start_minio_fs()
{
    "${MINIO[@]}" server "${WORK_DIR}/fs-disk" >"$WORK_DIR/fs-minio.log" 2>&1 &
    minio_pid=$!
    sleep 3

    echo "$minio_pid"
}

function start_minio_xl()
{
    "${MINIO[@]}" server "${WORK_DIR}/xl-disk1" "${WORK_DIR}/xl-disk2" "${WORK_DIR}/xl-disk3" "${WORK_DIR}/xl-disk4" >"$WORK_DIR/xl-minio.log" 2>&1 &
    minio_pid=$!
    sleep 3

    echo "$minio_pid"
}

function start_minio_dist()
{
    declare -a minio_pids
    "${MINIO[@]}" server --address=:9000 "http://127.0.0.1:9000${WORK_DIR}/dist-disk1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk4" >"$WORK_DIR/dist-minio-9000.log" 2>&1 &
    minio_pids[0]=$!
    "${MINIO[@]}" server --address=:9001 "http://127.0.0.1:9000${WORK_DIR}/dist-disk1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk4" >"$WORK_DIR/dist-minio-9001.log" 2>&1 &
    minio_pids[1]=$!
    "${MINIO[@]}" server --address=:9002 "http://127.0.0.1:9000${WORK_DIR}/dist-disk1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk4" >"$WORK_DIR/dist-minio-9002.log" 2>&1 &
    minio_pids[2]=$!
    "${MINIO[@]}" server --address=:9003 "http://127.0.0.1:9000${WORK_DIR}/dist-disk1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk4" >"$WORK_DIR/dist-minio-9003.log" 2>&1 &
    minio_pids[3]=$!

    sleep 30
    echo "${minio_pids[@]}"
}

function start_minio_gateway_s3()
{
    MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG \
                    "${MINIO[@]}" gateway s3 https://play.minio.io:9000 >"$WORK_DIR/minio-gateway-s3.log" 2>&1 &
    minio_pid=$!
    sleep 3

    echo "$minio_pid"
}

function run_test_fs()
{
    minio_pid="$(start_minio_fs)"

    (cd "$WORK_DIR" && "$FUNCTIONAL_TESTS")
    rv=$?

    kill "$minio_pid"
    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/fs-minio.log"
    fi
    rm -f "$WORK_DIR/fs-minio.log"

    return "$rv"
}

function run_test_xl()
{
    minio_pid="$(start_minio_xl)"

    (cd "$WORK_DIR" && "$FUNCTIONAL_TESTS")
    rv=$?

    kill "$minio_pid"
    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/xl-minio.log"
    fi
    rm -f "$WORK_DIR/xl-minio.log"

    return "$rv"
}

function run_test_dist()
{
    minio_pids=( $(start_minio_dist) )

    (cd "$WORK_DIR" && "$FUNCTIONAL_TESTS")
    rv=$?

    for pid in "${minio_pids[@]}"; do
        kill "$pid"
    done
    sleep 3

    if [ "$rv" -ne 0 ]; then
        echo "server1 log:"
        cat "$WORK_DIR/dist-minio-9000.log"
        echo "server2 log:"
        cat "$WORK_DIR/dist-minio-9001.log"
        echo "server3 log:"
        cat "$WORK_DIR/dist-minio-9002.log"
        echo "server4 log:"
        cat "$WORK_DIR/dist-minio-9003.log"
    fi

    rm -f "$WORK_DIR/dist-minio-9000.log" "$WORK_DIR/dist-minio-9001.log" "$WORK_DIR/dist-minio-9002.log" "$WORK_DIR/dist-minio-9003.log" 

   return "$rv"
}

function run_test_gateway_s3()
{
    minio_pid="$(start_minio_gateway_s3)"

    export ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
    export SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
    (cd "$WORK_DIR" && "$FUNCTIONAL_TESTS")
    rv=$?

    kill "$minio_pid"
    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/minio-gateway-s3.log"
    fi
    rm -f "$WORK_DIR/minio-gateway-s3.log"

    return "$rv"
}

function __init__()
{
    echo "Initializing environment"
    mkdir -p "$WORK_DIR"
    mkdir -p "$MINIO_CONFIG_DIR"
    mkdir -p "$MINT_DATA_DIR"

    if ! go get -u github.com/minio/mc; then
        echo "failed to download https://github.com/minio/mc"
        exit 1
    fi
    /bin/cp -a "$(go env GOPATH)"/bin/mc "$WORK_DIR/mc"

    chmod a+x "$WORK_DIR/mc"

    shred -n 1 -s 1M - 1>"$FILE_1_MB" 2>/dev/null
    shred -n 1 -s 65M - 1>"$FILE_65_MB" 2>/dev/null

    ## version is purposefully set to '3' for minio to migrate configuration file
    echo '{"version": "3", "credential": {"accessKey": "minio", "secretKey": "minio123"}, "region": "us-east-1"}' > "$MINIO_CONFIG_DIR/config.json"

    if ! wget -q -O "$FUNCTIONAL_TESTS" https://raw.githubusercontent.com/minio/mc/master/functional-tests.sh; then
        echo "failed to download https://raw.githubusercontent.com/minio/mc/master/functional-tests.sh"
        exit 1
    fi

    chmod a+x "$FUNCTIONAL_TESTS"
}

function main()
{
    echo "Testing in FS setup"
    if ! run_test_fs; then
        echo "FAILED"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "Testing in XL setup"
    if ! run_test_xl; then
        echo "FAILED"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Distribute XL setup"
    if ! run_test_dist; then
        echo "FAILED"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Gateway S3 setup"
    if ! run_test_gateway_s3; then
        echo "FAILED"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    rm -fr "$WORK_DIR"
}

( __init__ "$@" && main "$@" )
rv=$?
rm -fr "$WORK_DIR"
exit "$rv"
