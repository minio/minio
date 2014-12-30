#!/bin/sh

echo -n "Checking if proper environment variables are set.. "

echo ${GOROOT:?} 2>&1 >/dev/null
if [ $? -eq 1 ]; then
    echo "ERROR"
    echo "GOROOT environment variable missing, please refer to Go installation document"
    echo "https://github.com/Minio-io/minio/blob/master/BUILDDEPS.md#install-go-13"
    exit 1
fi

echo ${GOPATH:?} 2>&1 >/dev/null
if [ $? -eq 1 ]; then
    echo "ERROR"
    echo "GOPATH environment variable missing, please refer to Go installation document"
    echo "https://github.com/Minio-io/minio/blob/master/BUILDDEPS.md#install-go-13"
    exit 1
fi

echo "Done"
echo "Using GOPATH=${GOPATH} and GOROOT=${GOROOT}"

echo -n "Checking dependencies for Minio.. "

## Check all dependencies are present
MISSING=""

env git --version > /dev/null 2>&1
if [ $? -ne 0 ]; then
  MISSING="${MISSING} git"
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
echo "Done"
