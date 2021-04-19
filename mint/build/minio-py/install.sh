#!/bin/bash -e
#
#

MINIO_PY_VERSION=$(curl --retry 10 -Ls -o /dev/null -w "%{url_effective}" https://github.com/minio/minio-py/releases/latest | sed "s/https:\/\/github.com\/minio\/minio-py\/releases\/tag\///")
if [ -z "$MINIO_PY_VERSION" ]; then
    echo "unable to get minio-py version from github"
    exit 1
fi

test_run_dir="$MINT_RUN_CORE_DIR/minio-py"
pip3 install --user faker
pip3 install minio=="${MINIO_PY_VERSION}"
$WGET --output-document="$test_run_dir/tests.py" "https://raw.githubusercontent.com/minio/minio-py/${MINIO_PY_VERSION}/tests/functional/tests.py"
