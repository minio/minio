//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package triton

import (
	"fmt"
	"runtime"
)

// Version represents main version number of the current release
// of the Triton-go SDK.
const Version = "1.3.0"

// Prerelease adds a pre-release marker to the version.
//
// If this is "" (empty string) then it means that it is a final release.
// Otherwise, this is a pre-release such as "dev" (in development), "beta",
// "rc1", etc.
var Prerelease = "dev"

// UserAgent returns a Triton-go characteristic string that allows the
// network protocol peers to identify the version, release and runtime
// of the Triton-go client from which the requests originate.
func UserAgent() string {
	if Prerelease != "" {
		return fmt.Sprintf("triton-go/%s-%s (%s-%s; %s)", Version, Prerelease,
			runtime.GOARCH, runtime.GOOS, runtime.Version())
	}

	return fmt.Sprintf("triton-go/%s (%s-%s; %s)", Version, runtime.GOARCH,
		runtime.GOOS, runtime.Version())
}

// CloudAPIMajorVersion specifies the CloudAPI version compatibility
// for current release of the Triton-go SDK.
const CloudAPIMajorVersion = "8"
