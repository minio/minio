#!/bin/sh

echo -n "Checking dependencies for Minio.. "

## Check all dependencies are present
MISSING=""

env git --version > /dev/null 2>&1
if [ $? -ne 0 ]; then
  MISSING="${MISSING} git"
fi

env hg --version > /dev/null 2>&1
if [ $? -ne 0 ]; then
    MISSING="${MISSING} mercurial"
fi

env gcc --version > /dev/null 2>&1
if [ $? -ne 0 ]; then
    MISSING="${MISSING} build-essential"
fi

## If dependencies are missing, warn the user and abort
if [ "x${MISSING}" != "x" ]; then
  echo "ERROR"
  echo
  echo "The following build tools are missing:"
  echo
  echo "** ${MISSING} **"
  echo
  echo "Please install them "
  echo "$ sudo apt-get install ${MISSING}"
  echo
  exit 1
fi
echo "SUCCESS"
