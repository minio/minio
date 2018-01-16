//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package authentication

const authorizationHeaderFormat = `Signature keyId="%s",algorithm="%s",headers="%s",signature="%s"`

type Signer interface {
	DefaultAlgorithm() string
	KeyFingerprint() string
	Sign(dateHeader string) (string, error)
	SignRaw(toSign string) (string, string, error)
}
