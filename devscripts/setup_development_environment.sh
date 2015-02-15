#!/usr/bin/env bash

_init() {
    GO_VERSION="1.4"
    GIT_VERSION="1.0"
    PIP_VERSION="1.4"
    GCC_VERSION="4.0"
    YASM_VERSION="1.2.0"
    UNAME=$(uname -sm)
    MINIO_DEV=$HOME/minio-dev
}

die() {
    echo -e "\e[31m[!] $@\e[0m"; exit 1
}

msg() {
    echo -e "\e[93m[*] $@\e[0m"
}

call() {
    $@ 2>&1 | sed 's/^\(.*\)$/ | \1/g'
}

push_dir() {
    pushd $@ >/dev/null
}

pop_dir() {
    popd >/dev/null
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
check_version () {
    ## validate args
    [[ -z $1 ]] && return 3
    [[ -z $2 ]] && return 3

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

is_supported_arch() {
    local supported
    case ${UNAME##* } in
        "x86_64")
            supported=1
            ;;
        "i386")
            supported=0
            ;;
        *)
            supported=0
            ;;
    esac
    if [ $supported -eq 0 ]; then
        die "Invalid arch: ${UNAME} not supported, please use x86_64/amd64"
    fi
}

install_go() {
    msg "Downloading golang.."

    case ${UNAME%% *} in
        "Linux")
            os="linux"
            ;;
    esac
    case ${UNAME##* } in
        "x86_64")
            arch="amd64"
            ;;
    esac

    GOLANG_TARBALL_FNAME="go$GO_VERSION.$os-$arch.tar.gz"
    GOLANG_TARBALL_URL="https://storage.googleapis.com/golang/$GOLANG_TARBALL_FNAME"

    call curl --progress-bar -C - $GOLANG_TARBALL_URL -o $MINIO_DEV/dls/$GOLANG_TARBALL_FNAME

    call tar -xf $MINIO_DEV/dls/$GOLANG_TARBALL_FNAME -C $MINIO_DEV/deps
}

install_yasm() {

    msg "Downloading yasm.."

    YASM_TARBALL_FNAME="yasm-$YASM_VERSION.tar.gz"
    YASM_TARBALL_URL="http://www.tortall.net/projects/yasm/releases/$YASM_TARBALL_FNAME"

    curl --progress-bar -C - $YASM_TARBALL_URL -o $MINIO_DEV/dls/$YASM_TARBALL_FNAME

    call tar -xf $MINIO_DEV/dls/$YASM_TARBALL_FNAME -C $MINIO_DEV/deps/
    push_dir $MINIO_DEV/deps/yasm-$YASM_VERSION
    call ./configure
    call make
    pop_dir
}

setup_env() {
    python_version=$(python --version 2>&1 | sed 's/Python \([0-9]*.[0-9]*\).*/\1/')
    cat <<EOF > env.sh
#!/bin/sh

[[ -z \$GOROOT ]] && export GOROOT=\$MINIO_DEV/deps/go
export GOPATH=\$MINIO_DEV/mygo
export PATH=\$MINIO_DEV/deps/go/bin:\$MINIO_DEV/mygo/bin:\$MINIO_DEV/deps/yasm-\$YASM_VERSION:\$MINIO_DEV/deps/mkdocs/bin:\$GOPATH/bin:\$PATH
export PYTHONPATH=\$PYTHONPATH:\$MINIO_DEV/deps/mkdocs/lib/python\$python_version/site-packages/
EOF
}

install_mkdocs() {
    msg "Downloading mkdocs.."
    mkdir -p $MINIO_DEV/deps/mkdocs
    call pip install --install-option="--prefix=$MINIO_DEV/deps/mkdocs" mkdocs
}

install_minio_deps() {
    msg "Installing minio deps.."
    env go get github.com/tools/godep && echo "Installed godep"
    env go get golang.org/x/tools/cmd/cover && echo "Installed cover"
}

install_minio() {
    msg "Installing minio.."
    push_dir ${MINIO_DEV}/src
    call git clone "https://github.com/minio-io/minio"
    (cd minio; call make)
    pop_dir
}

main() {

    # Check supported arch
    is_supported_arch

    [[ -d ${MINIO_DEV} ]] || \
       die "You should have an empty working directory before you start.."

    mkdir -p ${MINIO_DEV}/{src,deps,dls,mygo}
    push_dir ${MINIO_DEV}

    check_version "$(env pip --version  | awk {'print $2'})" ${PIP_VERSION}
    [[ $? -ge 2 ]] && die "pip not installed"

    check_version "$(env gcc --version | sed 's/^.* \([0-9.]*\).*$/\1/' | head -1)" ${GCC_VERSION}
    [[ $? -ge 2 ]] && die "gcc not installed"

    check_version "$(env git --version | sed 's/^.* \([0-9.]*\).*$/\1/')" ${GIT_VERSION}
    [[ $? -ge 2 ]] && die "Git not installed"

    check_version "$(env go version 2>/dev/null | sed 's/^.* go\([0-9.]*\).*$/\1/')" ${GOLANG_VERSION}
    [[ $? -le 1 ]] && \
        [[ -z $GOROOT ]] && die "Please setup the goroot variable according to your current installation of golang." \
        || install_go

    check_version "$(env yasm --version 2>/dev/null | sed 's/^.* \([0-9.]*\).*$/\1/' | head -1)" ${YASM_VERSION}
    [[ $? -ge 2 ]] || install_yasm

    env mkdocs help >/dev/null 2>&1
    [[ $? -ne 0 ]] || install_mkdocs

    setup_env
    source env.sh
    install_minio_deps
    install_minio

    msg "--"
    msg "Run ''source env.sh'' to setup your work env."
}

# Putting main function at the end of the script ensures that the execution
# won't start until the script is entirely downloaded.
_init && main "$@"
