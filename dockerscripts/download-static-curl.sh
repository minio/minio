#!/bin/bash

ARCH=$(uname -m)

case $ARCH in
  aarch64) ARCH=aarch64 ;;
  x86_64)  ARCH=x86_64 ;;
  *)       echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

curl -fsSL "https://github.com/moparisthebest/static-curl/releases/latest/download/curl-linux-${ARCH}-musl-8.18.0.tar.xz" | tar -xJf - -O curl > /go/bin/curl
chmod +x /go/bin/curl