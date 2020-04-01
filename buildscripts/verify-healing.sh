#!/bin/bash
#
# MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
MINIO_CONFIG_DIR="$WORK_DIR/.minio"
MINIO=( "$PWD/minio" --config-dir "$MINIO_CONFIG_DIR" server )

function start_minio_3_node() {
    declare -a minio_pids
    declare -a ARGS
    export MINIO_ACCESS_KEY=minio
    export MINIO_SECRET_KEY=minio123

    start_port=$(shuf -i 10000-65000 -n 1)
    for i in $(seq 1 3); do
        ARGS+=("http://127.0.0.1:$[$start_port+$i]${WORK_DIR}/$i/1/ http://127.0.0.1:$[$start_port+$i]${WORK_DIR}/$i/2/ http://127.0.0.1:$[$start_port+$i]${WORK_DIR}/$i/3/ http://127.0.0.1:$[$start_port+$i]${WORK_DIR}/$i/4/ http://127.0.0.1:$[$start_port+$i]${WORK_DIR}/$i/5/ http://127.0.0.1:$[$start_port+$i]${WORK_DIR}/$i/6/")
    done

    "${MINIO[@]}" --address ":$[$start_port+1]" ${ARGS[@]} > "${WORK_DIR}/dist-minio-server1.log" 2>&1 &
    minio_pids[0]=$!
    disown "${minio_pids[0]}"

    "${MINIO[@]}" --address ":$[$start_port+2]" ${ARGS[@]} > "${WORK_DIR}/dist-minio-server2.log" 2>&1 &
    minio_pids[1]=$!
    disown "${minio_pids[1]}"

    "${MINIO[@]}" --address ":$[$start_port+3]" ${ARGS[@]} > "${WORK_DIR}/dist-minio-server3.log" 2>&1 &
    minio_pids[2]=$!
    disown "${minio_pids[2]}"

    sleep "$1"
    for pid in "${minio_pids[@]}"; do
        if ! kill "$pid"; then
            for i in $(seq 1 3); do
                echo "server$i log:"
                cat "${WORK_DIR}/dist-minio-server$i.log"
            done
            echo "FAILED"
            purge "$WORK_DIR"
            exit 1
        fi
        # forcibly killing, to proceed further properly.
        kill -9 "$pid"
        sleep 1 # wait 1sec per pid
    done
}


function check_online() {
    if grep -q 'Server switching to safe mode' ${WORK_DIR}/dist-minio-*.log; then
        echo "1"
    fi
}

function purge()
{
    rm -rf "$1"
}

function __init__()
{
    echo "Initializing environment"
    mkdir -p "$WORK_DIR"
    mkdir -p "$MINIO_CONFIG_DIR"

    ## version is purposefully set to '3' for minio to migrate configuration file
    echo '{"version": "3", "credential": {"accessKey": "minio", "secretKey": "minio123"}, "region": "us-east-1"}' > "$MINIO_CONFIG_DIR/config.json"
}

function perform_test() {
    start_minio_3_node 60

    echo "Testing Distributed Erasure setup healing of drives"
    echo "Remove the contents of the disks belonging to '${1}' erasure set"

    rm -rf ${WORK_DIR}/${1}/*/

    start_minio_3_node 60

    rv=$(check_online)
    if [ "$rv" == "1" ]; then
        pkill -9 minio
        for i in $(seq 1 3); do
            echo "server$i log:"
            cat "${WORK_DIR}/dist-minio-server$i.log"
        done
        echo "FAILED"
        purge "$WORK_DIR"
        exit 1
    fi
}

function main()
{
    perform_test "2"
    perform_test "1"
    perform_test "3"
}

( __init__ "$@" && main "$@" )
rv=$?
purge "$WORK_DIR"
exit "$rv"
