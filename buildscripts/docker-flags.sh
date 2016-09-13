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
version="2016-09-11T17:42:18Z"
release="RELEASE.2016-09-11T17-42-18Z"
commit="85e2d886bcb005d49f3876d6849a2b5a55e03cd3"

echo "-X ${minio}/cmd.Version=${version} -X ${minio}/cmd.ReleaseTag=${release} -X ${minio}/cmd.CommitID=${commit}"

