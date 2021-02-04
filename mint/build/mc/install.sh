#!/bin/bash -e
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

MC_VERSION=$(curl --retry 10 -Ls -o /dev/null -w "%{url_effective}" https://github.com/minio/mc/releases/latest | sed "s/https:\/\/github.com\/minio\/mc\/releases\/tag\///")
if [ -z "$MC_VERSION" ]; then
    echo "unable to get mc version from github"
    exit 1
fi

test_run_dir="$MINT_RUN_CORE_DIR/mc"
$WGET --output-document="${test_run_dir}/mc" "https://dl.minio.io/client/mc/release/linux-amd64/mc.${MC_VERSION}"
chmod a+x "${test_run_dir}/mc"

git clone --quiet https://github.com/minio/mc.git "$test_run_dir/mc.git"
(cd "$test_run_dir/mc.git"; git checkout --quiet "tags/${MC_VERSION}")
cp -a "${test_run_dir}/mc.git/functional-tests.sh" "$test_run_dir/"
rm -fr "$test_run_dir/mc.git"
