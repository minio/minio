#!/bin/bash
#
# Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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
    sleep 10

    echo "$minio_pid"
}

function start_minio_erasure()
{
    "${MINIO[@]}" server "${WORK_DIR}/erasure-disk1" "${WORK_DIR}/erasure-disk2" "${WORK_DIR}/erasure-disk3" "${WORK_DIR}/erasure-disk4" >"$WORK_DIR/erasure-minio.log" 2>&1 &
    minio_pid=$!
    sleep 15

    echo "$minio_pid"
}

function start_minio_erasure_sets()
{
    "${MINIO[@]}" server "${WORK_DIR}/erasure-disk-sets{1...32}" >"$WORK_DIR/erasure-minio-sets.log" 2>&1 &
    minio_pid=$!
    sleep 15

    echo "$minio_pid"
}

function start_minio_dist_erasure_sets()
{
    declare -a minio_pids
    "${MINIO[@]}" server --address=:9000 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9000.log" 2>&1 &
    minio_pids[0]=$!
    "${MINIO[@]}" server --address=:9001 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9001.log" 2>&1 &
    minio_pids[1]=$!
    "${MINIO[@]}" server --address=:9002 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9002.log" 2>&1 &
    minio_pids[2]=$!
    "${MINIO[@]}" server --address=:9003 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9003.log" 2>&1 &
    minio_pids[3]=$!
    "${MINIO[@]}" server --address=:9004 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9004.log" 2>&1 &
    minio_pids[4]=$!
    "${MINIO[@]}" server --address=:9005 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9005.log" 2>&1 &
    minio_pids[5]=$!
    "${MINIO[@]}" server --address=:9006 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9006.log" 2>&1 &
    minio_pids[6]=$!
    "${MINIO[@]}" server --address=:9007 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9007.log" 2>&1 &
    minio_pids[7]=$!
    "${MINIO[@]}" server --address=:9008 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9008.log" 2>&1 &
    minio_pids[8]=$!
    "${MINIO[@]}" server --address=:9009 "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets4" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets5" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets6" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets7" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets8" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets9" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets10" "http://127.0.0.1:9000${WORK_DIR}/dist-disk-sets11" "http://127.0.0.1:9001${WORK_DIR}/dist-disk-sets12" "http://127.0.0.1:9002${WORK_DIR}/dist-disk-sets13" "http://127.0.0.1:9003${WORK_DIR}/dist-disk-sets14" "http://127.0.0.1:9004${WORK_DIR}/dist-disk-sets15" "http://127.0.0.1:9005${WORK_DIR}/dist-disk-sets16" "http://127.0.0.1:9006${WORK_DIR}/dist-disk-sets17" "http://127.0.0.1:9007${WORK_DIR}/dist-disk-sets18" "http://127.0.0.1:9008${WORK_DIR}/dist-disk-sets19" "http://127.0.0.1:9009${WORK_DIR}/dist-disk-sets20" >"$WORK_DIR/dist-minio-9009.log" 2>&1 &
    minio_pids[9]=$!

    sleep 30
    echo "${minio_pids[@]}"
}

function start_minio_dist_erasure()
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

function run_test_erasure_sets() {
    minio_pid="$(start_minio_erasure_sets)"

    (cd "$WORK_DIR" && "$FUNCTIONAL_TESTS")
    rv=$?

    kill "$minio_pid"
    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/erasure-minio-sets.log"
    fi
    rm -f "$WORK_DIR/erasure-minio-sets.log"

    return "$rv"
}

function run_test_dist_erasure_sets()
{
    minio_pids=( $(start_minio_dist_erasure_sets) )

    (cd "$WORK_DIR" && "$FUNCTIONAL_TESTS")
    rv=$?

    for pid in "${minio_pids[@]}"; do
        kill "$pid"
    done
    sleep 3

    if [ "$rv" -ne 0 ]; then
        for i in $(seq 0 9); do
            echo "server$i log:"
            cat "$WORK_DIR/dist-minio-900$i.log"
        done
    fi

    for i in $(seq 0 9); do
        rm -f "$WORK_DIR/dist-minio-900$i.log"
    done

    return "$rv"
}

function run_test_erasure()
{
    minio_pid="$(start_minio_erasure)"

    (cd "$WORK_DIR" && "$FUNCTIONAL_TESTS")
    rv=$?

    kill "$minio_pid"
    sleep 3

    if [ "$rv" -ne 0 ]; then
        cat "$WORK_DIR/erasure-minio.log"
    fi
    rm -f "$WORK_DIR/erasure-minio.log"

    return "$rv"
}

function run_test_dist_erasure()
{
    minio_pids=( $(start_minio_dist_erasure) )

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

    echo "Testing in Erasure setup"
    if ! run_test_erasure; then
        echo "FAILED"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Distributed Erasure setup"
    if ! run_test_dist_erasure; then
        echo "FAILED"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Erasure setup as sets"
    if ! run_test_erasure_sets; then
        echo "FAILED"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "Testing in Distributed Erasure setup as sets"
    if ! run_test_dist_erasure_sets; then
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
