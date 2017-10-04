#!/bin/sh
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

# If command starts with an option, prepend minio.
if [ "${1}" != "minio" ]; then
    if [ -n "${1}" ]; then
        set -- minio "$@"
    fi
fi

## Look for docker secrets in default documented location.
docker_secrets_env() {
    local MINIO_ACCESS_KEY_FILE="/run/secrets/access_key"
    local MINIO_SECRET_KEY_FILE="/run/secrets/secret_key"

    if [ -f $MINIO_ACCESS_KEY_FILE -a -f $MINIO_SECRET_KEY_FILE ]; then
        if [ -f $MINIO_ACCESS_KEY_FILE ]; then
            export MINIO_ACCESS_KEY="$(cat "$MINIO_ACCESS_KEY_FILE")"
        fi
        if [ -f $MINIO_SECRET_KEY_FILE ]; then
            export MINIO_SECRET_KEY="$(cat "$MINIO_SECRET_KEY_FILE")"
        fi
    fi
}

## Set access env from secrets if necessary.
docker_secrets_env

exec "$@"
