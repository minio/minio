//
// Copyright (c) 2018, Joyent, Inc. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//

package authentication

// TestSigner represents an authentication key signer which we can use for
// testing purposes only. This will largely be a stub to send through client
// unit tests.
type TestSigner struct{}

// NewTestSigner constructs a new instance of test signer
func NewTestSigner() (Signer, error) {
	return &TestSigner{}, nil
}

func (s *TestSigner) DefaultAlgorithm() string {
	return ""
}

func (s *TestSigner) KeyFingerprint() string {
	return ""
}

func (s *TestSigner) Sign(dateHeader string) (string, error) {
	return "", nil
}

func (s *TestSigner) SignRaw(toSign string) (string, string, error) {
	return "", "", nil
}
