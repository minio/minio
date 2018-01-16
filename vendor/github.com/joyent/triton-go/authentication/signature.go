//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package authentication

import (
	"fmt"
	"regexp"
)

type httpAuthSignature interface {
	SignatureType() string
	String() string
}

func keyFormatToKeyType(keyFormat string) (string, error) {
	if keyFormat == "ssh-rsa" {
		return "rsa", nil
	}

	if keyFormat == "ssh-ed25519" {
		return "ed25519", nil
	}

	if regexp.MustCompile("^ecdsa-sha2-*").Match([]byte(keyFormat)) {
		return "ecdsa", nil
	}

	return "", fmt.Errorf("Unknown key format: %s", keyFormat)
}
