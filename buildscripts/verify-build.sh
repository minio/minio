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

function run_test_fs()
{
    minio_pid="$(start_minio_fs)"

    (cd "$WORK_DIR" && "$FUNCTIONAL_TESTS")
    rv=$?

    kill "$minio_pid"
    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat fs-minio.log
    fi
    rm -f fs-minio.log

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
        cat xl-minio.log
    fi
    rm -f xl-minio.log

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
        cat dist-minio-9000.log
        echo "server2 log:"
        cat dist-minio-9001.log
        echo "server3 log:"
        cat dist-minio-9002.log
        echo "server4 log:"
        cat dist-minio-9003.log
    fi

    rm -f dist-minio-900{0..3}.log

   return "$rv"
}

function __init__()
{
    echo "Initializing environment"
    mkdir -p "$WORK_DIR"
    mkdir -p "$MINIO_CONFIG_DIR"
    mkdir -p "$MINT_DATA_DIR"

    if ! wget -q -O "$WORK_DIR/mc" https://dl.minio.io/client/mc/release/linux-amd64/mc; then
        echo "failed to download https://dl.minio.io/client/mc/release/linux-amd64/mc"
        exit 1
    fi

    chmod a+x "$WORK_DIR/mc"

    base64 /dev/urandom | head -c 1048576 >"$FILE_1_MB"
    base64 /dev/urandom | head -c 68157440 >"$FILE_65_MB"

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
        echo "running test for FS setup failed"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "Testing in XL setup"
    if ! run_test_xl; then
        echo "running test for XL setup"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Distribute XL setup"
    if ! run_test_dist; then
        echo "running test for Distribute setup"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    rm -fr "$WORK_DIR"
}

( __init__ "$@" && main "$@" )
rv=$?
rm -fr "$WORK_DIR"
exit "$rv"
