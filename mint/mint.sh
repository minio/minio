#!/bin/bash
#
#  Mint (C) 2017, 2018 Minio, Inc.
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

CONTAINER_ID=$(grep -o -e '[0-f]\{12,\}' /proc/1/cpuset | awk '{print substr($1, 1, 12)}')
MINT_DATA_DIR=${MINT_DATA_DIR:-/mint/data}
MINT_MODE=${MINT_MODE:-core}
SERVER_REGION=${SERVER_REGION:-us-east-1}
ENABLE_HTTPS=${ENABLE_HTTPS:-0}
ENABLE_VIRTUAL_STYLE=${ENABLE_VIRTUAL_STYLE:-0}
GO111MODULE=on

if [ -z "$SERVER_ENDPOINT" ]; then
    SERVER_ENDPOINT="play.minio.io:9000"
    ACCESS_KEY="Q3AM3UQ867SPQQA43P2F"
    SECRET_KEY="zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
    ENABLE_HTTPS=1
fi

if [ "$ENABLE_VIRTUAL_STYLE" -eq 1 ]; then
        SERVER_IP="${SERVER_ENDPOINT%%:*}"
        SERVER_PORT="${SERVER_ENDPOINT/*:/}"
        # Check if SERVER_IP is actually IPv4 address
        octets=("${SERVER_IP//./ }")
        if [ "${#octets[@]}" -ne 4 ]; then
            echo "$SERVER_IP must be an IP address"
            exit 1
        fi
        for octet in "${octets[@]}"; do
	    if [ "$octet" -lt 0 ] 2>/dev/null || [ "$octet" -gt 255 ] 2>/dev/null; then
		echo "$SERVER_IP must be an IP address"
		exit 1
	    fi
        done
fi

ROOT_DIR="$PWD"
TESTS_DIR="$ROOT_DIR/run/core"

BASE_LOG_DIR="$ROOT_DIR/log"
LOG_FILE="log.json"
ERROR_FILE="error.log"
mkdir -p "$BASE_LOG_DIR"

function humanize_time()
{
    time="$1"
    days=$(( time / 60 / 60 / 24 ))
    hours=$(( time / 60 / 60 % 24 ))
    minutes=$(( time / 60 % 60 ))
    seconds=$(( time % 60 ))

    (( days > 0 )) && echo -n "$days days "
    (( hours > 0 )) && echo -n "$hours hours "
    (( minutes > 0 )) && echo -n "$minutes minutes "
    (( days > 0 || hours > 0 || minutes > 0 )) && echo -n "and "
    echo "$seconds seconds"
}

function run_test()
{
    if [ ! -d "$1" ]; then
        return 1
    fi

    start=$(date +%s)

    mkdir -p "$BASE_LOG_DIR/$sdk_name"

    (cd "$sdk_dir" && ./run.sh "$BASE_LOG_DIR/$LOG_FILE" "$BASE_LOG_DIR/$sdk_name/$ERROR_FILE")
    rv=$?
    end=$(date +%s)
    duration=$(humanize_time $(( end - start )))

    if [ "$rv" -eq 0 ]; then
        echo "done in $duration"
    else
        echo "FAILED in $duration"
        entry=$(tail -n 1 "$BASE_LOG_DIR/$LOG_FILE")
        status=$(jq -e -r .status <<<"$entry")
        jq_rv=$?
        if [ "$jq_rv" -ne 0 ]; then
            echo "$entry"
        fi
        ## Show error.log when status is empty or not "FAIL".
        ## This may happen when test run failed without providing logs.
        if [ "$jq_rv" -ne 0 ] || [ -z "$status" ] || ([ "$status" != "FAIL" ] && [ "$status" != "fail" ]); then
            cat "$BASE_LOG_DIR/$sdk_name/$ERROR_FILE"
        else
            jq . <<<"$entry"
        fi
    fi
    return $rv
}

function trust_s3_endpoint_tls_cert()
{
    # Download the public certificate from the server
    openssl s_client -showcerts -connect "$SERVER_ENDPOINT" </dev/null 2>/dev/null | \
	openssl x509 -outform PEM -out /usr/local/share/ca-certificates/s3_server_cert.crt || \
	exit 1

    # Load the certificate in the system
    update-ca-certificates --fresh >/dev/null

    # Ask different SDKs/tools to load system certificates
    export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
    export NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
}


function main()
{
    export MINT_DATA_DIR
    export MINT_MODE
    export SERVER_ENDPOINT
    export SERVER_IP
    export SERVER_PORT

    export ACCESS_KEY
    export SECRET_KEY
    export ENABLE_HTTPS
    export SERVER_REGION
    export ENABLE_VIRTUAL_STYLE
    export GO111MODULE

    echo "Running with"
    echo "SERVER_ENDPOINT:      $SERVER_ENDPOINT"
    echo "ACCESS_KEY:           $ACCESS_KEY"
    echo "SECRET_KEY:           ***REDACTED***"
    echo "ENABLE_HTTPS:         $ENABLE_HTTPS"
    echo "SERVER_REGION:        $SERVER_REGION"
    echo "MINT_DATA_DIR:        $MINT_DATA_DIR"
    echo "MINT_MODE:            $MINT_MODE"
    echo "ENABLE_VIRTUAL_STYLE: $ENABLE_VIRTUAL_STYLE"
    echo
    echo "To get logs, run 'docker cp ${CONTAINER_ID}:/mint/log /tmp/mint-logs'"
    echo

    [ "$ENABLE_HTTPS" == "1" ] && trust_s3_endpoint_tls_cert

    declare -a run_list
    sdks=( "$@" )

    if [ "$#" -eq 0 ]; then
        sdks=( $(ls "$TESTS_DIR") )
    fi

    for sdk in "${sdks[@]}"; do
        run_list=( "${run_list[@]}" "$TESTS_DIR/$sdk" )
    done

    count="${#run_list[@]}"
    i=0
    for sdk_dir in "${run_list[@]}"; do
        sdk_name=$(basename "$sdk_dir")
        (( i++ ))
        if [ ! -d "$sdk_dir" ]; then
            echo "Test $sdk_name not found. Exiting Mint."
            exit 1
        fi
        echo -n "($i/$count) Running $sdk_name tests ... "
        if ! run_test "$sdk_dir"; then
            (( i-- ))
        fi
    done

    ## Report when all tests in run_list are run
    if [ $i -eq "$count" ]; then
        echo -e "\nAll tests ran successfully"
    else
        echo -e "\nExecuted $i out of $count tests successfully."
        exit 1
    fi
}
main "$@"
