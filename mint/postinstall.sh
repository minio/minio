#!/bin/bash -e
#
#

export APT="apt --quiet --yes"

# remove all packages listed in remove-packages.list
xargs --arg-file="${MINT_ROOT_DIR}/remove-packages.list" apt --quiet --yes purge
${APT} autoremove

# remove unwanted files
rm -fr "$GOROOT" "$GOPATH/src" /var/lib/apt/lists/*

# flush to disk
sync
