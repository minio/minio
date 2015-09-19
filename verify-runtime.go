/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

var minGolangRuntimeVersion = "1.5.1"

// following code handles the current Golang release styles, we might have to update them in future
// if golang community divulges from the below formatting style.
const (
	betaRegexp = "beta[0-9]"
	rcRegexp   = "rc[0-9]"
)

func getNormalizedGolangVersion() string {
	version := strings.TrimPrefix(runtime.Version(), "go")
	br := regexp.MustCompile(betaRegexp)
	rr := regexp.MustCompile(rcRegexp)
	betaStr := br.FindString(version)
	version = strings.TrimRight(version, betaStr)
	rcStr := rr.FindString(version)
	version = strings.TrimRight(version, rcStr)
	return version
}

type golangVersion struct {
	major, minor, patch string
}

func newVersion(v string) golangVersion {
	ver := golangVersion{}
	verSlice := strings.Split(v, ".")
	if len(verSlice) < 2 {
		Fatalln("Version string missing major and minor versions, cannot proceed exiting.")
	}
	if len(verSlice) > 3 {
		Fatalf("Unknown Version style format, newVersion only supports ‘major.minor.patch’ not ‘%s’.\n", v)
	}
	ver.major = verSlice[0]
	ver.minor = verSlice[1]
	if len(verSlice) == 3 {
		ver.patch = verSlice[2]
	} else {
		ver.patch = "0"
	}
	return ver
}

func (v1 golangVersion) String() string {
	return fmt.Sprintf("%s%s%s", v1.major, v1.minor, v1.patch)
}

func (v1 golangVersion) Version() int {
	ver, e := strconv.Atoi(v1.String())
	if e != nil {
		Fatalln("Unable to parse version string.")
	}
	return ver
}

func (v1 golangVersion) LessThan(v2 golangVersion) bool {
	if v1.Version() < v2.Version() {
		return true
	}
	return false
}

func checkGolangRuntimeVersion() {
	v1 := newVersion(getNormalizedGolangVersion())
	v2 := newVersion(minGolangRuntimeVersion)
	if v1.LessThan(v2) {
		Errorln("Old Golang runtime version ‘" + v1.String() + "’ detected., ‘mc’ requires minimum go1.5.1 or later.")
	}
}
