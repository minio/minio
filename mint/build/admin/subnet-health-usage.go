/*
*
*  Mint, (C) 2021 Minio, Inc.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software

*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
 */

package main

import (
	"context"
	"time"
)

// testSubnetHealthUsage tests that the usage data (bucket count, object count and usage)
// is present (and non-zero) in the output of the healthinfo api. It is assumed that the
// test always runs against a cluster with non-zero usage.
func testSubnetHealthUsage() {
	function := "testSubnetHealthUsage"
	startTime := time.Now()
	args := map[string]interface{}{}

	si, err := adminClient.ServerInfo(context.Background())
	if err != nil {
		failureLog(function, args, startTime, "", "Subnet health server info failed", err).Fatal()
		return
	}

	if si.Buckets.Count == 0 {
		failureLog(function, args, startTime, "", "Bucket count is zero", err).Fatal()
		return
	}

	if si.Objects.Count == 0 {
		failureLog(function, args, startTime, "", "Objects count is zero", err).Fatal()
		return
	}

	if si.Usage.Size == 0 {
		failureLog(function, args, startTime, "", "Usage is zero", err).Fatal()
		return
	}

	successLog(function, args, startTime).Info()
}
