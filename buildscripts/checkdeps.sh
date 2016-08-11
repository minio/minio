#!/usr/bin/env bash
#
# Minio Cloud Storage, (C) 2015 Minio, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

_init() {

    shopt -s extglob

    ## Minimum required versions for build dependencies
    GIT_VERSION="1.0"
    GO_VERSION="1.6"
    OSX_VERSION="10.8"
    UNAME=$(uname -sm)

    ## Check all dependencies are present
    MISSING=""
}

readlink() {
    TARGET_FILE=$1

    cd `dirname $TARGET_FILE`
    TARGET_FILE=`basename $TARGET_FILE`

    # Iterate down a (possible) chain of symlinks
    while [ -L "$TARGET_FILE" ]
    do
        TARGET_FILE=$(env readlink $TARGET_FILE)
        cd `dirname $TARGET_FILE`
        TARGET_FILE=`basename $TARGET_FILE`
    done

    # Compute the canonicalized name by finding the physical path
    # for the directory we're in and appending the target file.
    PHYS_DIR=`pwd -P`
    RESULT=$PHYS_DIR/$TARGET_FILE
    echo $RESULT
}

###
#
# Takes two arguments
# arg1: version number in `x.x.x` format
# arg2: version number in `x.x.x` format
#
# example: check_version "$version1" "$version2"
#
# returns:
# 0 - Installed version is equal to required
# 1 - Installed version is greater than required
# 2 - Installed version is lesser than required
# 3 - If args have length zero
#
####
check_version() {
    ## validate args
    [[ -z "$1" ]] && return 3
    [[ -z "$2" ]] && return 3

    if [[ $1 == $2 ]]; then
        return 0
    fi

    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++)); do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++)); do
        if [[ -z ${ver2[i]} ]]; then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]})); then

            return 1
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]})); then
            ## Installed version is lesser than required - Bad condition
            return 2
        fi
    done
    return 0
}

check_golang_env() {
    echo ${GOROOT:?} 2>&1 >/dev/null
    if [ $? -eq 1 ]; then
        echo "ERROR"
        echo "GOROOT environment variable missing, please refer to Go installation document"
        echo "https://github.com/minio/minio/blob/master/INSTALLGO.md#install-go-13"
        exit 1
    fi

    echo ${GOPATH:?} 2>&1 >/dev/null
    if [ $? -eq 1 ]; then
        echo "ERROR"
        echo "GOPATH environment variable missing, please refer to Go installation document"
        echo "https://github.com/minio/minio/blob/master/INSTALLGO.md#install-go-13"
        exit 1
    fi

    local go_binary_path=$(which go)

    if [ -z "${go_binary_path}" ] ; then
        echo "Cannot find go binary in your PATH configuration, please refer to Go installation document"
        echo "https://github.com/minio/minio/blob/master/INSTALLGO.md#install-go-13"
        exit -1
    fi

    local new_go_binary_path=${go_binary_path}
    if [ -h "${go_binary_path}" ]; then
        new_go_binary_path=$(readlink ${go_binary_path})
    fi

    if [[ !"$(dirname ${new_go_binary_path})" =~ *"${GOROOT%%*(/)}"* ]] ; then
        echo "The go binary found in your PATH configuration does not belong to the Go installation pointed by your GOROOT environment," \
            "please refer to Go installation document"
        echo "https://github.com/minio/minio/blob/master/INSTALLGO.md#install-go-13"
        exit -1
    fi
}

is_supported_os() {
    case ${UNAME%% *} in
        "Linux")
            os="linux"
            ;;
        "FreeBSD")
            os="freebsd"
            ;;
        "Darwin")
            osx_host_version=$(env sw_vers -productVersion)
            check_version "${osx_host_version}" "${OSX_VERSION}"
            [[ $? -ge 2 ]] && die "Minimum OSX version supported is ${OSX_VERSION}"
            ;;
        "*")
            echo "Exiting.. unsupported operating system found"
            exit 1;
    esac
}

is_supported_arch() {
    local supported
    case ${UNAME##* } in
        "x86_64" | "amd64")
            supported=1
            ;;
        "arm"*)
            supported=1
            ;;
        *)
            supported=0
            ;;
    esac
    if [ $supported -eq 0 ]; then
        echo "Invalid arch: ${UNAME} not supported, please use x86_64/amd64"
        exit 1;
    fi
}

check_deps() {
    check_version "$(env go version 2>/dev/null | sed 's/^.* go\([0-9.]*\).*$/\1/')" "${GO_VERSION}"
    if [ $? -ge 2 ]; then
        MISSING="${MISSING} golang(${GO_VERSION})"
    fi

    check_version "$(env git --version 2>/dev/null | sed -e 's/^.* \([0-9.\].*\).*$/\1/' -e 's/^\([0-9.\]*\).*/\1/g')" "${GIT_VERSION}"
    if [ $? -ge 2 ]; then
        MISSING="${MISSING} git"
    fi
}

main() {
    echo -n "Check for supported arch.. "
    is_supported_arch

    echo -n "Check for supported os.. "
    is_supported_os

    echo -n "Checking if proper environment variables are set.. "
    check_golang_env

    echo "Done"
    echo "Using GOPATH=${GOPATH} and GOROOT=${GOROOT}"

    echo -n "Checking dependencies for Minio.. "
    check_deps

    ## If dependencies are missing, warn the user and abort
    if [ "x${MISSING}" != "x" ]; then
        echo "ERROR"
        echo
        echo "The following build tools are missing:"
        echo
        echo "** ${MISSING} **"
        echo
        echo "Please install them "
        echo "${MISSING}"
        echo
        echo "Follow https://docs.minio.io/docs/how-to-install-golang for further instructions"
        exit 1
    fi
    echo "Done"
}

_init && main "$@"
