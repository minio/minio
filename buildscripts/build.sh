#!/bin/bash

_init() {
    # Save release LDFLAGS
    LDFLAGS=$(go run buildscripts/gen-ldflags.go)

    # Extract release tag
    release_tag=$(echo $LDFLAGS | awk {'print $4'} | cut -f2 -d=)

    # Verify release tag.
    if [ -z "$release_tag" ]; then
        echo "Release tag cannot be empty. Please check return value of \`go run buildscripts/gen-ldflags.go\`"
        exit 1;
    fi

    # Extract release string.
    release_str=$(echo $MC_RELEASE | tr '[:upper:]' '[:lower:]')

    # Verify release string.
    if [ -z "$release_str" ]; then
        echo "Release string cannot be empty. Please set \`MC_RELEASE\` env variable."
        exit 1;
    fi

    # List of supported architectures
    SUPPORTED_OSARCH='linux/386 linux/amd64 linux/arm windows/386 windows/amd64 darwin/amd64'
}

go_build() {
    local osarch=$1
    os=$(echo $osarch | cut -f1 -d'/')
    arch=$(echo $osarch | cut -f2 -d'/')
    package=$(go list -f '{{.ImportPath}}')
    echo -n "-->"
    printf "%15s:%s\n" "${osarch}" "${package}"
    GO15VENDOREXPERIMENT=1 GOOS=$os GOARCH=$arch go build --ldflags "${LDFLAGS}" -o $release_str/$os-$arch/$(basename $package).$release_tag
}

main() {
    # Build releases.
    echo "Executing $release_str builds for OS: ${SUPPORTED_OSARCH}"
    for osarch in ${SUPPORTED_OSARCH}; do
        go_build ${osarch}
    done
}

# Run main.
_init && main
