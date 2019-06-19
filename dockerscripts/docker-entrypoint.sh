#!/bin/sh
#
# MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

export MINIO_USERNAME=${MINIO_USERNAME:-"minio"}
export MINIO_GROUPNAME=${MINIO_GROUPNAME:-"minio"}

# If command starts with an option, prepend minio.
if [ "${1}" != "minio" ]; then
    if [ -n "${1}" ]; then
        set -- minio "$@"
    fi
fi

## Look for docker secrets in default documented location.
docker_secrets_env() {
    ACCESS_KEY_FILE="/run/secrets/$MINIO_ACCESS_KEY_FILE"
    SECRET_KEY_FILE="/run/secrets/$MINIO_SECRET_KEY_FILE"

    if [ -f "$ACCESS_KEY_FILE" ] && [ -f "$SECRET_KEY_FILE" ]; then
        if [ -f "$ACCESS_KEY_FILE" ]; then
            MINIO_ACCESS_KEY="$(cat "$ACCESS_KEY_FILE")"
            export MINIO_ACCESS_KEY
        fi
        if [ -f "$SECRET_KEY_FILE" ]; then
            MINIO_SECRET_KEY="$(cat "$SECRET_KEY_FILE")"
            export MINIO_SECRET_KEY
        fi
    fi
}

## Set SSE_MASTER_KEY from docker secrets if provided
docker_sse_encryption_env() {
    SSE_MASTER_KEY_FILE="/run/secrets/$MINIO_SSE_MASTER_KEY_FILE"

    if [ -f "$SSE_MASTER_KEY_FILE" ]; then
        MINIO_SSE_MASTER_KEY="$(cat "$SSE_MASTER_KEY_FILE")"
        export MINIO_SSE_MASTER_KEY

    fi
}

## Create UID/GID based on available environment variables.
docker_set_uid_gid() {
    addgroup -S "$MINIO_GROUPNAME" >/dev/null 2>&1 && \
        adduser -S -G "$MINIO_GROUPNAME" "$MINIO_USERNAME" >/dev/null 2>&1
}

# su-exec to requested user, if user cannot be requested
# existing user is used automatically.
docker_switch_user() {
    owner=$(check-user "$@")
    if [ "${owner}" != "${MINIO_USERNAME}:${MINIO_GROUPNAME}" ]; then
        ## Print the message only if we are not using non-default username:groupname.
        if [ "${MINIO_USERNAME}:${MINIO_GROUPNAME}" != "minio:minio" ]; then
            echo "Requested username/group ${MINIO_USERNAME}:${MINIO_GROUPNAME} cannot be used"
            echo "Found existing data with user ${owner}, we will continue and use ${owner} instead."
            return
        fi
    fi
    # check if su-exec is allowed, if yes proceed proceed.
    if su-exec "${owner}" "/bin/ls" >/dev/null 2>&1; then
        exec su-exec "${owner}" "$@"
    fi
    # fallback
    exec "$@"
}

## Set access env from secrets if necessary.
docker_secrets_env

## Set sse encryption from secrets if necessary.
docker_sse_encryption_env

## User Input UID and GID
docker_set_uid_gid

## Switch to user if applicable.
docker_switch_user "$@"
