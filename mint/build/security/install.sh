#!/bin/bash -e
#
#

test_run_dir="$MINT_RUN_CORE_DIR/security"
(cd "$test_run_dir" && GO111MODULE=on CGO_ENABLED=0 go build -o tls-tests)
