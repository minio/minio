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
    local ACCESS_KEY_FILE="/run/secrets/$MINIO_ACCESS_KEY_FILE"
    local SECRET_KEY_FILE="/run/secrets/$MINIO_SECRET_KEY_FILE"

    if [ -f $ACCESS_KEY_FILE -a -f $SECRET_KEY_FILE ]; then
        if [ -f $ACCESS_KEY_FILE ]; then
            export MINIO_ACCESS_KEY="$(cat "$ACCESS_KEY_FILE")"
        fi
        if [ -f $SECRET_KEY_FILE ]; then
            export MINIO_SECRET_KEY="$(cat "$SECRET_KEY_FILE")"
        fi
    fi
}

## Create UID/GID based on available environment variables.
docker_set_uid_gid() {
    if  [ -z "$MINIO_GID" ] &&  [ -z "$MINIO_UID" ] ; then
        MINIO_GID="190"
        MINIO_UID="190"
    fi
    addgroup -g "$MINIO_GID" -S minio && adduser -u "$MINIO_UID" -S -G minio minio
}

# switch to non root (minio) user if there isn't an existing .minio.sys directory or
# if its an empty data directory. We dont switch to minio user if there is an existing
# data directory, this is because we'll need to chown all the files -- which may take
# too long.
docker_switch_non_root() {
    for var in "$@"
    do
        if [ "${var}" = "minio" ] || [ "${var}" = "server" ] ; then
            continue
        fi

        if [ "${var}" = "gateway" ]; then
            mode="${var}"
            continue
        fi
        
        # Updating mode to "gateway nas"
        if [ "${var}" = "nas" ]; then
            mode="${mode}" "${var}"
            continue
        fi

        # If mode is gateway (other than gateway nas) set owner as minio and break       
        if [ "${mode}" = "gateway" ]; then
            owner="minio"
            break
        fi
        
        # Strip scheme and hostname if present in an arg
        if [ "${var:0:4}" == "http" ]; then
            var="/$(printf -- "%s" "${var}" | cut -d/ -f4-)"
        fi   

        # This checks for old deployment
        if [ -d "${var}" ]; then
            # If .minio.sys doesn't exist set owner as minio
            if ! [ -d "${var}/.minio.sys" ]; then
                # change owner to minio user
                chown minio:minio "${var}"
                owner="minio" 
            else 
                # If .minio.sys exists, then check if owner's id is equal to MINIO_UID passed
                if [  "$(id -u "$(ls -ld "${var}/.minio.sys" | awk '{print $3}')")" == "$MINIO_UID" ]; then
                    owner="minio" 
                fi
            fi
            break  
        else 
        # This is the case of new deployment i.e. the folder is not present
            owner="minio"
            break
        fi
    done

    if [ "${owner}" = "minio"  ]; then
       # run as minio user
       exec su-exec $owner "$@"       
    else
       # else run as root user
       exec "$@"
    fi
}

## Set access env from secrets if necessary.
docker_secrets_env

## User Input UID and GID
docker_set_uid_gid

## Switch to non root user minio if applicable.
docker_switch_non_root "$@"
