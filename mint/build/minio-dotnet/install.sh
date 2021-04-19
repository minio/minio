#!/bin/bash
#
#

set -e

MINIO_DOTNET_SDK_PATH="$MINT_RUN_CORE_DIR/minio-dotnet"

MINIO_DOTNET_SDK_VERSION=$(curl --retry 10 -Ls -o /dev/null -w "%{url_effective}" https://github.com/minio/minio-dotnet/releases/latest | sed "s/https:\/\/github.com\/minio\/minio-dotnet\/releases\/tag\///")
if [ -z "$MINIO_DOTNET_SDK_VERSION" ]; then
    echo "unable to get minio-dotnet version from github"
    exit 1
fi

out_dir="$MINIO_DOTNET_SDK_PATH/out"
if [ -z "$out_dir" ]; then
    mkdir "$out_dir"
fi

temp_dir="$MINIO_DOTNET_SDK_PATH/temp"
git clone --quiet https://github.com/minio/minio-dotnet.git "${temp_dir}/minio-dotnet.git/"
(cd "${temp_dir}/minio-dotnet.git"; git checkout --quiet "tags/${MINIO_DOTNET_SDK_VERSION}")

cp -a "${temp_dir}/minio-dotnet.git/Minio.Functional.Tests/"* "${MINIO_DOTNET_SDK_PATH}/"
rm -fr "${temp_dir}"

cd "$MINIO_DOTNET_SDK_PATH"
dotnet restore /p:Configuration=Mint
dotnet publish --runtime ubuntu.18.04-x64 --output out /p:Configuration=Mint
