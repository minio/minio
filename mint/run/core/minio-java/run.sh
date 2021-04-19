#!/bin/bash
#
#

# handle command line arguments
if [ $# -ne 2 ]; then
    echo "usage: run.sh <OUTPUT-LOG-FILE> <ERROR-LOG-FILE>"
    exit 1
fi

output_log_file="$1"
error_log_file="$2"

# run tests
endpoint="http://$SERVER_ENDPOINT"
if [ "$ENABLE_HTTPS" -eq 1 ]; then
    endpoint="https://$SERVER_ENDPOINT"
fi

java -Xmx4096m -Xms256m -cp "/mint/run/core/minio-java/*:." FunctionalTest \
    "$endpoint" "$ACCESS_KEY" "$SECRET_KEY" "$SERVER_REGION" "$RUN_ON_FAIL" 1>>"$output_log_file" 2>"$error_log_file"
