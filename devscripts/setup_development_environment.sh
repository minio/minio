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

check_version() {
    local version=$1 check=$2
    local highest=$(echo -e "$version\n$check" | sort -nrt. -k1,1 -k2,2 -k3,3 | head -1)
    [[ "$highest" = "$version" ]] && return 0
    return 1
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
    [[ $? -ne 0 ]] && die "pip not installed"

    check_version "$(env gcc --version | sed 's/^.* \([0-9.]*\).*$/\1/' | head -1)" ${GCC_VERSION}
    [[ $? -ne 0 ]] && die "gcc not installed"

    check_version "$(env git --version | sed 's/^.* go\([0-9.]*\).*$/\1/')" ${GIT_VERSION}
    [[ $? -ne 1 ]] && die "Git not installed"

    check_version "$(env go version 2>/dev/null | sed 's/^.* go\([0-9.]*\).*$/\1/')" ${GOLANG_VERSION}
    [[ $? -eq 0 ]] && \
        [[ -z $GOROOT ]] && die "Please setup the goroot variable according to your current installation of golang." \
        || install_go

    check_version "$(env yasm --version 2>/dev/null)" ${YASM_VERSION}
    [[ $? -eq 0 ]] || install_yasm

    env mkdocs help >/dev/null 2>&1
    [[ $? -eq 0 ]] || install_mkdocs

    setup_env
    source env.sh
    install_minio_deps
    install_minio

    msg "--"
    msg "Run ''source env.sh'' to setup your work env."
}

# Putting main function at the end of the script ensures that the execution
# won't start until the script is entirely downloaded.
_init "$@" && main "$@"
