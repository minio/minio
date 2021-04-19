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
python "/mint/run/core/minio-py/tests.py" 1>>"$output_log_file" 2>"$error_log_file"
