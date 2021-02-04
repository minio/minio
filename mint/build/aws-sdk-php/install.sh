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

test_run_dir="$MINT_RUN_CORE_DIR/aws-sdk-php"
$WGET --output-document=- https://getcomposer.org/installer | php -- --install-dir="$test_run_dir"
php "$test_run_dir/composer.phar" --working-dir="$test_run_dir" install
