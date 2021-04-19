#!/bin/bash -e
#
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
