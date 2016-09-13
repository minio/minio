#!/usr/bin/env bash
#
# Minio Cloud Storage, (C) 2016 Minio, Inc.
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

minio="github.com/minio/minio"
release=`git tag --points-at HEAD`
date=`git tag --points-at HEAD | sed 's/RELEASE.//g' | cut -f1 -d'T'`
time=`git tag --points-at HEAD | sed 's/RELEASE.//g' | cut -f2 -d'T' | sed 's/-/:/g'`
version="${date}T${time}"
commit=`git rev-parse HEAD`

echo "-X ${minio}/cmd.Version=${version} -X ${minio}/cmd.ReleaseTag=${release} -X ${minio}/cmd.CommitID=${commit}"

