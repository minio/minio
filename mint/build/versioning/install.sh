#!/bin/bash -e
#
#

test_run_dir="$MINT_RUN_CORE_DIR/versioning"
test_build_dir="$MINT_RUN_BUILD_DIR/versioning"

(cd "$test_build_dir" && GO111MODULE=on CGO_ENABLED=0 go build --ldflags "-s -w" -o "$test_run_dir/tests")
