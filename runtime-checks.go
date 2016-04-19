/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	"github.com/hashicorp/go-version"
	"github.com/minio/mc/pkg/console"
)

// isContainerized returns true if we are inside a containerized environment.
func isContainerized() bool {
	// Docker containers contain ".dockerenv" at their root path.
	if _, e := os.Stat("/.dockerenv"); e == nil {
		return true
	}

	// Check if cgroup policies for init process contains docker string.
	if cgroupData, e := ioutil.ReadFile("/proc/1/cgroup"); e == nil {
		if strings.Contains(string(cgroupData), "/docker-") {
			return true
		}
	}

	// Check if env var explicitly set
	if allow := os.Getenv("ALLOW_CONTAINER_ROOT"); allow == "1" || strings.ToLower(allow) == "true" {
		return true
	}

	/* Add checks for non-docker containers here. */
	return false
}

// check if minimum Go version is met.
func checkGoVersion() {
	// Current version.
	curVersion, e := version.NewVersion(runtime.Version()[2:])
	if e != nil {
		console.Fatalln("Unable to determine current go version.", e)
	}

	// Prepare version constraint.
	constraints, e := version.NewConstraint(minGoVersion)
	if e != nil {
		console.Fatalln("Unable to check go version.")
	}

	// Check for minimum version.
	if !constraints.Check(curVersion) {
		console.Fatalln(fmt.Sprintf("Please recompile Minio with Golang version %s.", minGoVersion))
	}
}
