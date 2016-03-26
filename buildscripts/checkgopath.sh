#!/usr/bin/env bash
#
# Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

_init() {

    shopt -s extglob

    PWD=$(pwd -P)
    GOPATH=$(cd "$(go env GOPATH)" ; env pwd -P)
}

main() {
    echo "Checking if project is at ${GOPATH}"
    for minio in $(echo ${GOPATH} | tr ':' ' '); do
        if [ ! -d ${minio}/src/github.com/minio/minio ]; then
            echo "Project not found in ${minio}, please follow instructions provided at https://github.com/minio/minio/blob/master/CONTRIBUTING.md#setup-your-minio-github-repository" \
                && exit 1
        fi
        if [ "x${minio}/src/github.com/minio/minio" != "x${PWD}" ]; then
            echo "Build outside of ${minio}, two source checkouts found. Exiting." && exit 1
        fi
    done
}

_init && main

