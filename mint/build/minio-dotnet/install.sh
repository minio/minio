#!/bin/bash
#
#  Mint (C) 2017 Minio, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
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

curl https://raw.githubusercontent.com/minio/minio-dotnet/"${MINIO_DOTNET_SDK_VERSION}"/Minio.Functional.Tests/FunctionalTest.cs > "${MINIO_DOTNET_SDK_PATH}/FunctionalTest.cs"
curl https://raw.githubusercontent.com/minio/minio-dotnet/"${MINIO_DOTNET_SDK_VERSION}"/Minio.Functional.Tests/MintLogger.cs > "${MINIO_DOTNET_SDK_PATH}/MintLogger.cs"
curl https://raw.githubusercontent.com/minio/minio-dotnet/"${MINIO_DOTNET_SDK_VERSION}"/Minio.Functional.Tests/JsonNetLogger.cs > "${MINIO_DOTNET_SDK_PATH}/JsonNetLogger.cs"
curl https://raw.githubusercontent.com/minio/minio-dotnet/"${MINIO_DOTNET_SDK_VERSION}"/Minio.Functional.Tests/Program.cs > "${MINIO_DOTNET_SDK_PATH}/Program.cs"
curl https://raw.githubusercontent.com/minio/minio-dotnet/"${MINIO_DOTNET_SDK_VERSION}"/Minio.Functional.Tests/RandomStreamGenerator.cs > "${MINIO_DOTNET_SDK_PATH}/RandomStreamGenerator.cs"
curl https://raw.githubusercontent.com/minio/minio-dotnet/"${MINIO_DOTNET_SDK_VERSION}"/Minio.Functional.Tests/Minio.Functional.Tests.csproj > "${MINIO_DOTNET_SDK_PATH}/Minio.Functional.Tests.csproj"

cd "$MINIO_DOTNET_SDK_PATH"
dotnet restore /p:Configuration=Mint
dotnet publish --runtime ubuntu.16.04-x64 --output out /p:Configuration=Mint
