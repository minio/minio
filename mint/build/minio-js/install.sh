#!/bin/bash -e
#
#

MINIO_JS_VERSION=$(curl --retry 10 -Ls -o /dev/null -w "%{url_effective}" https://github.com/minio/minio-js/releases/latest | sed "s/https:\/\/github.com\/minio\/minio-js\/releases\/tag\///")
if [ -z "$MINIO_JS_VERSION" ]; then
    echo "unable to get minio-js version from github"
    exit 1
fi

test_run_dir="$MINT_RUN_CORE_DIR/minio-js"
mkdir "${test_run_dir}/test"
$WGET --output-document="${test_run_dir}/test/functional-tests.js" "https://raw.githubusercontent.com/minio/minio-js/${MINIO_JS_VERSION}/src/test/functional/functional-tests.js"
npm --prefix "$test_run_dir" install --save "minio@$MINIO_JS_VERSION"
npm --prefix "$test_run_dir" install
