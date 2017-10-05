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

# helper function to check if a string contains another string
string_match () { 
    case "$2" in *$1*)
        return 0
    esac
    return 1
}

# switch to non root (minio) user if there isn't an existing Minio data directory or
# if its an empty data directory. We dont switch to minio user if there is an existing
# data directory, this is because we'll need to chown all the files -- which may take 
# too long, ruining the user's experience.
docker_switch_non_root() {
    arguments_processed=0
    # loop through all the arguements
    for var in "$@"
    do
        echo "1"
        # ignore minio, server, gateway or minio server flags
        if [ "${var}" = "minio" ] || [ "${var}" = "server" ] \
            || [ "${var}" = "gateway" ] || string_match '--*' "${var}" || \
            string_match ':*' "${var}" ; then
            # count as processed
            arguments_processed=$((arguments_processed+1))
            echo "2"
            continue
        fi
        # strip scheme and hostname if present in an arg
        if [ "${var:0:4}" = "http" ]; then
            echo "3"
            echo "${var}"
            if ! string_match "$(hostname)" "${var}"; then 
                echo "Didnt match $(hostname) ""${var}"
                continue
            fi
            var=$(printf -- "%s" "${var}" | cut -d/ -f4-)
        fi
        # if the given directory exists
        if [ -d "${var}" ];then
            echo "directory $var exists"
            # if the directory is empty
            if [ "$(ls -A $var)" = "" ]; then
                echo "directory $var was empty so chowning"
                # change owner to minio user
                chown -R minio:minio "${var}"
            fi 
            # find the owner of the directory
            owner=$(ls -ld "${var}" | awk '{print $3}')
            # if minio is the owner, count as processed
            if [ "${owner}" = "minio" ];then
                echo "previous owner = minio"
                arguments_processed=$((arguments_processed+1))
            fi
        else
            echo "Created $var directory"
            # if the directory doesn't exist, this is a new deployment.
            mkdir -p "${var}"
            # change owner to minio user
            chown minio:minio "${var}"
            # count as processed
            arguments_processed=$((arguments_processed+1))
        fi
    done
    # if all arguements are processed, and allowed, switch to minio user
    if [ "${arguments_processed}" = $# ]; then
        exec su-exec minio "$@"
    else 
        # else run as root user 
        exec "$@"
    fi
}

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
## Switch to non root user minio if applicable.
docker_switch_non_root "$@"
