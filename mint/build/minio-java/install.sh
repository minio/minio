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

MINIO_JAVA_VERSION=$(curl --retry 10 -s "https://repo1.maven.org/maven2/io/minio/minio/maven-metadata.xml" | sed -n "/<latest>/{s/<.[^>]*>//g;p;q}" | sed "s/  *//g")
if [ -z "$MINIO_JAVA_VERSION" ]; then
    echo "unable to get latest minio-java version from maven"
    exit 1
fi

test_run_dir="$MINT_RUN_CORE_DIR/minio-java"
git clone --quiet https://github.com/minio/minio-java.git "$test_run_dir/minio-java.git"
(cd "$test_run_dir/minio-java.git"; git checkout --quiet "tags/${MINIO_JAVA_VERSION}")
$WGET --output-document="$test_run_dir/minio-${MINIO_JAVA_VERSION}-all.jar" "https://repo1.maven.org/maven2/io/minio/minio/${MINIO_JAVA_VERSION}/minio-${MINIO_JAVA_VERSION}-all.jar"
javac -cp "$test_run_dir/minio-${MINIO_JAVA_VERSION}-all.jar" "${test_run_dir}/minio-java.git/functional"/*.java
cp -a "${test_run_dir}/minio-java.git/functional"/*.class "$test_run_dir/"
rm -fr "$test_run_dir/minio-java.git"
