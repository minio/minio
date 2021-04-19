#!/bin/bash -e
#
#

export MINT_ROOT_DIR=${MINT_ROOT_DIR:-/mint}
export MINT_RUN_CORE_DIR="$MINT_ROOT_DIR/run/core"
export MINT_RUN_BUILD_DIR="$MINT_ROOT_DIR/build"
export MINT_RUN_SECURITY_DIR="$MINT_ROOT_DIR/run/security"
export WGET="wget --quiet --no-check-certificate"

"${MINT_ROOT_DIR}"/create-data-files.sh
"${MINT_ROOT_DIR}"/preinstall.sh

# install mint app packages
for pkg in "$MINT_ROOT_DIR/build"/*/install.sh; do
    echo "Running $pkg"
    $pkg
done

"${MINT_ROOT_DIR}"/postinstall.sh
