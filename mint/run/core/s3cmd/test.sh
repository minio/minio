#!/bin/bash
#
# Mint (C) 2017-2020 Minio, Inc.
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



if [ -n "$MINT_MODE" ]; then
    if [ -z "${MINT_DATA_DIR+x}" ]; then
        echo "MINT_DATA_DIR not defined"
        exit 1
    fi
    if [ -z "${SERVER_ENDPOINT+x}" ]; then
        echo "SERVER_ENDPOINT not defined"
        exit 1
    fi
    if [ -z "${ACCESS_KEY+x}" ]; then
        echo "ACCESS_KEY not defined"
        exit 1
    fi
    if [ -z "${SECRET_KEY+x}" ]; then
        echo "SECRET_KEY not defined"
        exit 1
    fi
fi

if [ -z "${SERVER_ENDPOINT+x}" ]; then
    SERVER_ENDPOINT="play.minio.io:9000"
    ACCESS_KEY="Q3AM3UQ867SPQQA43P2F"
    SECRET_KEY="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
    ENABLE_HTTPS=1
    SERVER_REGION="us-east-1"
fi

WORK_DIR="$PWD"
DATA_DIR="$MINT_DATA_DIR"
if [ -z "$MINT_MODE" ]; then
    WORK_DIR="$PWD/.run-$RANDOM"
    DATA_DIR="$WORK_DIR/data"
fi

FILE_1_MB="$DATA_DIR/datafile-1-MB"
FILE_65_MB="$DATA_DIR/datafile-65-MB"
declare FILE_1_MB_MD5SUM
declare FILE_65_MB_MD5SUM

BUCKET_NAME="s3cmd-test-bucket-$RANDOM"
S3CMD=$(which s3cmd)
declare -a S3CMD_CMD

function get_md5sum()
{
    filename="$FILE_1_MB"
    out=$(md5sum "$filename" 2>/dev/null)
    rv=$?
    if [ "$rv" -eq 0 ]; then
        awk '{ print $1 }' <<< "$out"
    fi

    return "$rv"
}

function get_time()
{
    date +%s%N
}

function get_duration()
{
    start_time=$1
    end_time=$(get_time)

    echo $(( (end_time - start_time) / 1000000 ))
}

function log_success()
{
    if [ -n "$MINT_MODE" ]; then
        printf '{"name": "s3cmd", "duration": "%d", "function": "%s", "status": "PASS"}\n' "$(get_duration "$1")" "$2"
    fi
}

function show()
{
    if [ -z "$MINT_MODE" ]; then
        func_name="$1"
        echo "Running $func_name()"
    fi
}

function fail()
{
    rv="$1"
    shift

    if [ "$rv" -ne 0 ]; then
        echo "$@"
    fi

    return "$rv"
}

function assert()
{
    expected_rv="$1"
    shift
    start_time="$1"
    shift
    func_name="$1"
    shift

    err=$("$@" 2>&1)
    rv=$?
    if [ "$rv" -ne 0 ] && [ "$expected_rv" -eq 0 ]; then
        if [ -n "$MINT_MODE" ]; then
            err=$(printf '%s' "$err" | python -c 'import sys,json; print(json.dumps(sys.stdin.read()))')
            ## err is already JSON string, no need to double quote
            printf '{"name": "s3cmd", "duration": "%d", "function": "%s", "status": "FAIL", "error": %s}\n' "$(get_duration "$start_time")" "$func_name" "$err"
        else
            echo "s3cmd: $func_name: $err"
        fi

        exit "$rv"
    fi

    return 0
}

function assert_success() {
    assert 0 "$@"
}

function assert_failure() {
    assert 1 "$@"
}

function s3cmd_cmd()
{
    cmd=( "${S3CMD_CMD[@]}" "$@" )
    "${cmd[@]}"
    rv=$?
    return "$rv"
}

function check_md5sum()
{
    expected_checksum="$1"
    shift
    filename="$*"

    checksum="$(get_md5sum "$filename")"
    rv=$?
    if [ "$rv" -ne 0 ]; then
        echo "unable to get md5sum for $filename"
        return "$rv"
    fi

    if [ "$checksum" != "$expected_checksum" ]; then
        echo "$filename: md5sum mismatch"
        return 1
    fi

    return 0
}

function test_make_bucket()
{
    show "${FUNCNAME[0]}"

    start_time=$(get_time)
    bucket_name="s3cmd-test-bucket-$RANDOM"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd mb "s3://${bucket_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rb "s3://${bucket_name}"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function test_make_bucket_error() {
    show "${FUNCNAME[0]}"

    start_time=$(get_time)
    bucket_name="S3CMD-test%bucket%$RANDOM"
    assert_failure "$start_time" "${FUNCNAME[0]}" s3cmd_cmd mb "s3://${bucket_name}"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function setup()
{
    start_time=$(get_time)
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd mb "s3://${BUCKET_NAME}"
}

function teardown()
{
    start_time=$(get_time)
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rm --force --recursive "s3://${BUCKET_NAME}"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rb --force "s3://${BUCKET_NAME}"
}

function test_put_object()
{
    show "${FUNCNAME[0]}"

    start_time=$(get_time)
    object_name="s3cmd-test-object-$RANDOM"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd put "${FILE_1_MB}" "s3://${BUCKET_NAME}/${object_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rm "s3://${BUCKET_NAME}/${object_name}"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function test_put_object_error()
{
    show "${FUNCNAME[0]}"
    start_time=$(get_time)

    object_long_name=$(printf "s3cmd-test-object-%01100d" 1)
    assert_failure "$start_time" "${FUNCNAME[0]}" s3cmd_cmd put "${FILE_1_MB}" "s3://${BUCKET_NAME}/${object_long_name}"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function test_put_object_multipart()
{
    show "${FUNCNAME[0]}"

    start_time=$(get_time)
    object_name="s3cmd-test-object-$RANDOM"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd put "${FILE_65_MB}" "s3://${BUCKET_NAME}/${object_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rm "s3://${BUCKET_NAME}/${object_name}"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function test_get_object()
{
    show "${FUNCNAME[0]}"

    start_time=$(get_time)
    object_name="s3cmd-test-object-$RANDOM"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd put "${FILE_1_MB}" "s3://${BUCKET_NAME}/${object_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd get "s3://${BUCKET_NAME}/${object_name}" "${object_name}.downloaded"
    assert_success "$start_time" "${FUNCNAME[0]}" check_md5sum "$FILE_1_MB_MD5SUM" "${object_name}.downloaded"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rm "s3://${BUCKET_NAME}/${object_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" rm -f "${object_name}.downloaded"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function test_get_object_error()
{
    show "${FUNCNAME[0]}"

    start_time=$(get_time)
    object_name="s3cmd-test-object-$RANDOM"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd put "${FILE_1_MB}" "s3://${BUCKET_NAME}/${object_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rm "s3://${BUCKET_NAME}/${object_name}"
    assert_failure "$start_time" "${FUNCNAME[0]}" s3cmd_cmd get "s3://${BUCKET_NAME}/${object_name}" "${object_name}.downloaded"
    assert_success "$start_time" "${FUNCNAME[0]}" rm -f "${object_name}.downloaded"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function test_get_object_multipart()
{
    show "${FUNCNAME[0]}"

    start_time=$(get_time)
    object_name="s3cmd-test-object-$RANDOM"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd put "${FILE_65_MB}" "s3://${BUCKET_NAME}/${object_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd get "s3://${BUCKET_NAME}/${object_name}" "${object_name}.downloaded"
    assert_success "$start_time" "${FUNCNAME[0]}" check_md5sum "$FILE_65_MB_MD5SUM" "${object_name}.downloaded"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rm "s3://${BUCKET_NAME}/${object_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" rm -f "${object_name}.downloaded"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function test_sync_list_objects()
{
    show "${FUNCNAME[0]}"

    start_time=$(get_time)
    bucket_name="s3cmd-test-bucket-$RANDOM"
    object_name="s3cmd-test-object-$RANDOM"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd mb "s3://${bucket_name}"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd sync "$DATA_DIR/" "s3://${bucket_name}"

    diff -bB <(ls "$DATA_DIR") <("${S3CMD_CMD[@]}" ls "s3://${bucket_name}" | awk '{print $4}' | sed "s/s3:*..${bucket_name}.//g") >/dev/null 2>&1
    assert_success "$start_time" "${FUNCNAME[0]}" fail $? "sync and list differs"
    assert_success "$start_time" "${FUNCNAME[0]}" s3cmd_cmd rb --force --recursive "s3://${bucket_name}"

    log_success "$start_time" "${FUNCNAME[0]}"
}

function run_test()
{
    test_make_bucket
    test_make_bucket_error

    setup

    test_put_object
    test_put_object_error
    test_put_object_multipart
    test_get_object
    test_get_object_multipart
    test_sync_list_objects

    teardown
}

function __init__()
{
    set -e

    S3CMD_CONFIG_DIR="/tmp/.s3cmd-$RANDOM"
    mkdir -p $S3CMD_CONFIG_DIR
    S3CMD_CONFIG_FILE="$S3CMD_CONFIG_DIR/s3cfg"

    # configure s3cmd
    cat > $S3CMD_CONFIG_FILE <<EOF
signature_v2 = False
host_base = $SERVER_ENDPOINT
host_bucket = $SERVER_ENDPOINT
bucket_location = $SERVER_REGION
use_https = $ENABLE_HTTPS
access_key = $ACCESS_KEY
secret_key = $SECRET_KEY
EOF

    # For Mint, setup is already done.  For others, setup the environment
    if [ -z "$MINT_MODE" ]; then
        mkdir -p "$WORK_DIR"
        mkdir -p "$DATA_DIR"

        # If s3cmd executable binary is not available in current directory, use it in the path.
        if [ ! -x "$S3CMD" ]; then
            echo "'s3cmd' executable binary not found in current directory and in path"
            exit 1
        fi
    fi

    if [ ! -x "$S3CMD" ]; then
        echo "$S3CMD executable binary not found"
        exit 1
    fi

    S3CMD_CMD=( "${S3CMD}" --config "$S3CMD_CONFIG_FILE" )

    if [ ! -e "$FILE_1_MB" ]; then
        shred -n 1 -s 1MB - >"$FILE_1_MB"
    fi

    if [ ! -e "$FILE_65_MB" ]; then
        shred -n 1 -s 65MB - >"$FILE_65_MB"
    fi

    set -E
    set -o pipefail

    FILE_1_MB_MD5SUM="$(get_md5sum "$FILE_1_MB")"
    rv=$?
    if [ $rv -ne 0 ]; then
        echo "unable to get md5sum of $FILE_1_MB"
        exit 1
    fi

    FILE_65_MB_MD5SUM="$(get_md5sum "$FILE_65_MB")"
    rv=$?
    if [ $rv -ne 0 ]; then
        echo "unable to get md5sum of $FILE_65_MB"
        exit 1
    fi

    set +e
}

function main()
{
    ( run_test )
    rv=$?

    rm -fr "$S3CMD_CONFIG_FILE"
    if [ -z "$MINT_MODE" ]; then
        rm -fr "$WORK_DIR" "$DATA_DIR"
    fi

    exit "$rv"
}

__init__ "$@"
main "$@"
