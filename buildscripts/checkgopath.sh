#!/usr/bin/env bash
#
# Minio Cloud Storage, (C) 2015-2018 Minio, Inc.
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

main() {
    gopath=$(go env GOPATH)
    IFS=':' read -r -a paths <<< "$gopath"
    for path in "${paths[@]}"; do
        minio_path="$path/src/github.com/minio/minio"
        if [ -d "$minio_path" ]; then
            if [ "$minio_path" -ef "$PWD" ]; then
               exit 0
            fi
        fi
    done

    echo "ERROR"
    echo "Project not found in ${gopath}."
    echo "Follow instructions at https://github.com/minio/minio/blob/master/CONTRIBUTING.md#setup-your-minio-github-repository"
    exit 1
}

main
