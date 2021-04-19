#!/bin/bash -e
#
#

SPOTBUGS_VERSION="4.2.2" ## needed since 8.0.2 release
JUNIT_VERSION="4.12" ## JUNIT version
MINIO_JAVA_VERSION=$(curl --retry 10 -s "https://repo1.maven.org/maven2/io/minio/minio/maven-metadata.xml" | sed -n "/<latest>/{s/<.[^>]*>//g;p;q}" | sed "s/  *//g")
if [ -z "$MINIO_JAVA_VERSION" ]; then
    echo "unable to get latest minio-java version from maven"
    exit 1
fi

test_run_dir="$MINT_RUN_CORE_DIR/minio-java"
git clone --quiet https://github.com/minio/minio-java.git "$test_run_dir/minio-java.git"
(cd "$test_run_dir/minio-java.git"; git checkout --quiet "tags/${MINIO_JAVA_VERSION}")
$WGET --output-document="$test_run_dir/minio-${MINIO_JAVA_VERSION}-all.jar" "https://repo1.maven.org/maven2/io/minio/minio/${MINIO_JAVA_VERSION}/minio-${MINIO_JAVA_VERSION}-all.jar"
$WGET --output-document="$test_run_dir/spotbugs-annotations-${SPOTBUGS_VERSION}.jar" "https://repo1.maven.org/maven2/com/github/spotbugs/spotbugs-annotations/${SPOTBUGS_VERSION}/spotbugs-annotations-${SPOTBUGS_VERSION}.jar"
$WGET --output-document="$test_run_dir/junit-${JUNIT_VERSION}.jar" "https://repo1.maven.org/maven2/junit/junit/${JUNIT_VERSION}/junit-${JUNIT_VERSION}.jar"
javac -cp "$test_run_dir/minio-${MINIO_JAVA_VERSION}-all.jar:$test_run_dir/spotbugs-annotations-${SPOTBUGS_VERSION}.jar:$test_run_dir/junit-${JUNIT_VERSION}.jar" "${test_run_dir}/minio-java.git/functional"/*.java
cp -a "${test_run_dir}/minio-java.git/functional"/*.class "$test_run_dir/"
rm -fr "$test_run_dir/minio-java.git"
