#!/bin/bash
#
#  Mint (C) 2017 Minio, Inc.
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

# handle command line arguments
if [ $# -ne 2 ]; then
    echo "usage: run.sh <OUTPUT-LOG-FILE> <ERROR-LOG-FILE>"
    exit -1
fi

output_log_file="$1"
error_log_file="$2"

proto="http"
if [[ ${ENABLE_HTTPS} -eq 1 ]]; then
    proto="https"
fi

forcePathStyle="true"
if [[ ${ENABLE_VIRTUAL_STYLE} -eq 1 ]]; then
    forcePathStyle="false"
fi

# run tests
/mint/run/core/s3select/s3select -endpoint="${proto}://${SERVER_ENDPOINT}" -access_key="${ACCESS_KEY}" -secret_key="${SECRET_KEY}" -force_path_style="${forcePathStyle}" | grep '^{' 1>>"$output_log_file" 2>"$error_log_file"
