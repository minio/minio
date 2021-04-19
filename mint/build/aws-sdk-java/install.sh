#!/bin/bash -e
#
#

test_run_dir="$MINT_RUN_CORE_DIR/aws-sdk-java"

cd "$(dirname "$(realpath "$0")")"

ant init-ivy && \
    ant resolve && \
    ant compile && \
    ant jar

cp build/jar/FunctionalTests.jar "$test_run_dir/"

rm -rf lib/ build/

