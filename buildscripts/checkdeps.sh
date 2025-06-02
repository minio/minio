#!/usr/bin/env bash
#

_init() {

	shopt -s extglob

	## Minimum required versions for build dependencies
	GIT_VERSION="1.0"
	GO_VERSION="1.16"
	OSX_VERSION="10.8"
	KNAME=$(uname -s)
	ARCH=$(uname -m)
	case "${KNAME}" in
	SunOS)
		ARCH=$(isainfo -k)
		;;
	esac
}

## FIXME:
## In OSX, 'readlink -f' option does not exist, hence
## we have our own readlink -f behavior here.
## Once OSX has the option, below function is good enough.
##
## readlink() {
##     return /bin/readlink -f "$1"
## }
##
readlink() {
	TARGET_FILE=$1

	cd $(dirname $TARGET_FILE)
	TARGET_FILE=$(basename $TARGET_FILE)

	# Iterate down a (possible) chain of symlinks
	while [ -L "$TARGET_FILE" ]; do
		TARGET_FILE=$(env readlink $TARGET_FILE)
		cd $(dirname $TARGET_FILE)
		TARGET_FILE=$(basename $TARGET_FILE)
	done

	# Compute the canonicalized name by finding the physical path
	# for the directory we're in and appending the target file.
	PHYS_DIR=$(pwd -P)
	RESULT=$PHYS_DIR/$TARGET_FILE
	echo $RESULT
}

## FIXME:
## In OSX, 'sort -V' option does not exist, hence
## we have our own version compare function.
## Once OSX has the option, below function is good enough.
##
## check_minimum_version() {
##     versions=($(echo -e "$1\n$2" | sort -V))
##     return [ "$1" == "${versions[0]}" ]
## }
##
check_minimum_version() {
	IFS='.' read -r -a varray1 <<<"$1"
	IFS='.' read -r -a varray2 <<<"$2"

	for i in "${!varray1[@]}"; do
		if [[ ${varray1[i]} -lt ${varray2[i]} ]]; then
			return 0
		elif [[ ${varray1[i]} -gt ${varray2[i]} ]]; then
			return 1
		fi
	done

	return 0
}

assert_is_supported_arch() {
	case "${ARCH}" in
	x86_64 | amd64 | aarch64 | ppc64le | arm* | s390x | loong64 | loongarch64 | riscv64)
		return
		;;
	*)
		echo "Arch '${ARCH}' is not supported. Supported Arch: [x86_64, amd64, aarch64, ppc64le, arm*, s390x, loong64, loongarch64, riscv64]"
		exit 1
		;;
	esac
}

assert_is_supported_os() {
	case "${KNAME}" in
	Linux | FreeBSD | OpenBSD | NetBSD | DragonFly | SunOS)
		return
		;;
	Darwin)
		osx_host_version=$(env sw_vers -productVersion)
		if ! check_minimum_version "${OSX_VERSION}" "${osx_host_version}"; then
			echo "OSX version '${osx_host_version}' is not supported. Minimum supported version: ${OSX_VERSION}"
			exit 1
		fi
		return
		;;
	*)
		echo "OS '${KNAME}' is not supported. Supported OS: [Linux, FreeBSD, OpenBSD, NetBSD, Darwin, DragonFly]"
		exit 1
		;;
	esac
}

assert_check_golang_env() {
	if ! which go >/dev/null 2>&1; then
		echo "Cannot find go binary in your PATH configuration, please refer to Go installation document at https://golang.org/doc/install"
		exit 1
	fi

	installed_go_version=$(go version | sed 's/^.* go\([0-9.]*\).*$/\1/')
	if ! check_minimum_version "${GO_VERSION}" "${installed_go_version}"; then
		echo "Go runtime version '${installed_go_version}' is unsupported. Minimum supported version: ${GO_VERSION} to compile."
		exit 1
	fi
}

assert_check_deps() {
	# support unusual Git versions such as: 2.7.4 (Apple Git-66)
	installed_git_version=$(git version | perl -ne '$_ =~ m/git version (.*?)( |$)/; print "$1\n";')
	if ! check_minimum_version "${GIT_VERSION}" "${installed_git_version}"; then
		echo "Git version '${installed_git_version}' is not supported. Minimum supported version: ${GIT_VERSION}"
		exit 1
	fi
}

main() {
	## Check for supported arch
	assert_is_supported_arch

	## Check for supported os
	assert_is_supported_os

	## Check for Go environment
	assert_check_golang_env

	## Check for dependencies
	assert_check_deps
}

_init && main "$@"
