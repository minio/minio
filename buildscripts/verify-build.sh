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

WORK_DIR="$PWD/verify-$RANDOM"
MINIO_CONFIG_DIR="$WORK_DIR/.minio"
MC_CONFIG_DIR="$WORK_DIR/.mc"
SERVER_ALIAS="myminio"
ACCESS_KEY="minio"
SECRET_KEY="minio123"
BUCKET_NAME="mc-test-bucket-$RANDOM"
MINIO="$PWD/minio"
MC_CMD="$WORK_DIR/mc"
FILE_1_MB="$WORK_DIR/datafile-1-MB"
FILE_65_MB="$WORK_DIR/datafile-65-MB"

function create_minio_config()
{
    ## version is purposefully set to '3' for minio to migrate configuration file
    echo '{"version": "3", "credential": {"accessKey": "minio", "secretKey": "minio123"}, "region": "us-east-1"}' > "$MINIO_CONFIG_DIR/config.json"
}

function assert_run_mc_command()
{
    outfile="$WORK_DIR/cmd.out.$RANDOM"

    rv=0
    if ! "${MC_CMD}" --config-folder "$MC_CONFIG_DIR" --quiet --no-color "$@" >"$outfile" 2>&1; then
        echo "failed to run command: ${MC_CMD}" "$@"
        cat "$outfile"
        rv=1
    fi

    rm -f "$outfile"

    if [ "$rv" -ne 0 ]; then
        exit 1
    fi
}

function assert_download_mc()
{
    if ! wget -q -O "${MC_CMD}" https://dl.minio.io/client/mc/release/linux-amd64/mc; then
        echo "failed to download 'mc'"
        exit 1
    fi

    chmod a+x "${MC_CMD}"
    assert_run_mc_command config host add "${SERVER_ALIAS}" http://127.0.0.1:9000 minio minio123
}

function start_minio_fs()
{
    "$MINIO" --config-dir "$MINIO_CONFIG_DIR" server "${WORK_DIR}/fs-disk" >"$WORK_DIR/fs-minio.log" 2>&1 &
    minio_pid=$!
    sleep 3

    echo "$minio_pid"
}

function start_minio_xl()
{
    "$MINIO" --config-dir "$MINIO_CONFIG_DIR" server "${WORK_DIR}/xl-disk1" "${WORK_DIR}/xl-disk2" "${WORK_DIR}/xl-disk3" "${WORK_DIR}/xl-disk4" >"$WORK_DIR/xl-minio.log" 2>&1 &
    minio_pid=$!
    sleep 3

    echo "$minio_pid"
}

function start_minio_dist()
{
    declare -a minio_pids
    "$MINIO" --config-dir "$MINIO_CONFIG_DIR" server --address=:9000 "http://127.0.0.1:9000${WORK_DIR}/dist-disk1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk4" >"$WORK_DIR/dist-minio-9000.log" 2>&1 &
    minio_pids[0]=$!
    "$MINIO" --config-dir "$MINIO_CONFIG_DIR" server --address=:9001 "http://127.0.0.1:9000${WORK_DIR}/dist-disk1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk4" >"$WORK_DIR/dist-minio-9001.log" 2>&1 &
    minio_pids[1]=$!
    "$MINIO" --config-dir "$MINIO_CONFIG_DIR" server --address=:9002 "http://127.0.0.1:9000${WORK_DIR}/dist-disk1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk4" >"$WORK_DIR/dist-minio-9002.log" 2>&1 &
    minio_pids[2]=$!
    "$MINIO" --config-dir "$MINIO_CONFIG_DIR" server --address=:9003 "http://127.0.0.1:9000${WORK_DIR}/dist-disk1" "http://127.0.0.1:9001${WORK_DIR}/dist-disk2" "http://127.0.0.1:9002${WORK_DIR}/dist-disk3" "http://127.0.0.1:9003${WORK_DIR}/dist-disk4" >"$WORK_DIR/dist-minio-9003.log" 2>&1 &
    minio_pids[3]=$!

    sleep 30
    echo "${minio_pids[@]}"
}

function test_make_bucket()
{
    bucket_name="mc-test-bucket-$RANDOM"
    assert_run_mc_command mb "${SERVER_ALIAS}/${bucket_name}"
    assert_run_mc_command rm "${SERVER_ALIAS}/${bucket_name}"
}

function setup()
{
    assert_run_mc_command mb "${SERVER_ALIAS}/${BUCKET_NAME}"
}

function teardown()
{
    assert_run_mc_command rm --force --recursive "${SERVER_ALIAS}/${BUCKET_NAME}"
}

function test_put_object()
{
    object_name="mc-test-object-$RANDOM"
    assert_run_mc_command cp "${FILE_1_MB}" "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}"
    assert_run_mc_command rm "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}"
}

function test_put_object_multipart()
{
    object_name="mc-test-object-$RANDOM"
    assert_run_mc_command cp "${FILE_65_MB}" "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}"
    assert_run_mc_command rm "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}"
}

function test_get_object()
{
    object_name="mc-test-object-$RANDOM"
    assert_run_mc_command cp "${FILE_1_MB}" "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}"
    assert_run_mc_command cp "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}" "${object_name}.downloaded"
    assert_run_mc_command rm "${object_name}.downloaded" "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}"
}

function test_get_object_multipart()
{
    object_name="mc-test-object-$RANDOM"
    assert_run_mc_command cp "${FILE_65_MB}" "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}"
    assert_run_mc_command cp "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}" "${object_name}.downloaded"
    assert_run_mc_command rm "${object_name}.downloaded" "${SERVER_ALIAS}/${BUCKET_NAME}/${object_name}"
}

function run_test()
{
    test_make_bucket

    setup

    test_put_object
    test_put_object_multipart
    test_get_object
    test_get_object_multipart

    teardown
}

function run_test_fs()
{
    minio_pid="$(start_minio_fs)"

    ( run_test )
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

    ( run_test )
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

    ( run_test )
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

function main()
{
    mkdir "$WORK_DIR"
    mkdir "$MINIO_CONFIG_DIR"
    mkdir "$MC_CONFIG_DIR"

    if ! base64 /dev/urandom | head -c 1048576 >"$FILE_1_MB"; then
        echo "unable to create 1 MiB file"
        exit 1
    fi

    if ! base64 /dev/urandom | head -c 68157440 >"$FILE_65_MB"; then
        echo "unable to create 65 MiB file"
        exit 1
    fi

    create_minio_config

    echo "downloading mc"
    assert_download_mc

    cd "$WORK_DIR"

    echo "running test in FS setup"
    if ! run_test_fs; then
        echo "running test for FS setup failed"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "running test in XL setup"
    if ! run_test_xl; then
        echo "running test for XL setup"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    echo "running test in Distribute XL setup"
    if ! run_test_dist; then
        echo "running test for Distribute setup"
        rm -fr "$WORK_DIR"
        exit 1
    fi

    rm -fr "$WORK_DIR"
}

set -e
main "$@"
