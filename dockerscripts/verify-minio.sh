#!/bin/sh
#
# MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

set -e

if [ ! -x "/usr/bin/minio" ]; then
    echo "minio executable binary not found refusing to proceed"
    exit 1
fi

verify_sha256sum() {
    echo "verifying binary checksum"
    echo "$(awk '{print $1}' /usr/bin/minio.sha256sum)  /usr/bin/minio" | sha256sum -c
}

verify_signature() {
    echo "verifying binary signature"
    minisign -VQm /usr/bin/minio -P RWTx5Zr1tiHQLwG9keckT0c45M3AGeHD6IvimQHpyRywVWGbP1aVSGav
}

main() {
    verify_sha256sum

    verify_signature
}

main "$@"
