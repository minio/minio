//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package authentication

import (
	"encoding/base64"
)

type rsaSignature struct {
	hashAlgorithm string
	signature     []byte
}

func (s *rsaSignature) SignatureType() string {
	return s.hashAlgorithm
}

func (s *rsaSignature) String() string {
	return base64.StdEncoding.EncodeToString(s.signature)
}

func newRSASignature(signatureBlob []byte) (*rsaSignature, error) {
	return &rsaSignature{
		hashAlgorithm: "rsa-sha1",
		signature:     signatureBlob,
	}, nil
}
