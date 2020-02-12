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

## Set KMS_MASTER_KEY from docker secrets if provided
docker_kms_encryption_env() {
    KMS_MASTER_KEY_FILE="/run/secrets/$MINIO_KMS_MASTER_KEY_FILE"

    if [ -f "$KMS_MASTER_KEY_FILE" ]; then
        MINIO_KMS_MASTER_KEY="$(cat "$KMS_MASTER_KEY_FILE")"
        export MINIO_KMS_MASTER_KEY

    fi
}

## Legacy
## Set SSE_MASTER_KEY from docker secrets if provided
docker_sse_encryption_env() {
    SSE_MASTER_KEY_FILE="/run/secrets/$MINIO_SSE_MASTER_KEY_FILE"

    if [ -f "$SSE_MASTER_KEY_FILE" ]; then
        MINIO_SSE_MASTER_KEY="$(cat "$SSE_MASTER_KEY_FILE")"
        export MINIO_SSE_MASTER_KEY

    fi
}

# su-exec to requested user, if service cannot run exec will fail.
docker_switch_user() {
    if [ ! -z "${MINIO_USERNAME}" ] && [ ! -z "${MINIO_GROUPNAME}" ]; then
        addgroup -S "$MINIO_GROUPNAME" >/dev/null 2>&1 && \
            adduser -S -G "$MINIO_GROUPNAME" "$MINIO_USERNAME" >/dev/null 2>&1

        exec su-exec "${MINIO_USERNAME}:${MINIO_GROUPNAME}" "$@"
    else
        # fallback
        exec "$@"
    fi
}

## Set access env from secrets if necessary.
docker_secrets_env

## Set kms encryption from secrets if necessary.
docker_kms_encryption_env

## Set sse encryption from secrets if necessary. Legacy
docker_sse_encryption_env

## Switch to user if applicable.
docker_switch_user "$@"
