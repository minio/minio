//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package triton

import (
	"github.com/joyent/triton-go/authentication"
)

// Universal package used for defining configuration used across all client
// constructors.

// ClientConfig is a placeholder/input struct around the behavior of configuring
// a client constructor through the implementation's runtime environment
// (SDC/MANTA env vars).
type ClientConfig struct {
	TritonURL   string
	MantaURL    string
	AccountName string
	Username    string
	Signers     []authentication.Signer
}
