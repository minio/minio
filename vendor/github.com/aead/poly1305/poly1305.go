// Copyright (c) 2016 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

// Package poly1305 implements Poly1305 one-time message authentication code
// defined in RFC 7539..
//
// Poly1305 is a fast, one-time authentication function. It is infeasible for an
// attacker to generate an authenticator for a message without the key.
// However, a key must only be used for a single message. Authenticating two
// different messages with the same key allows an attacker to forge
// authenticators for other messages with the same key.
package poly1305 // import "github.com/aead/poly1305"

import (
	"crypto/subtle"
	"errors"
)

// TagSize is the size of the poly1305 authentication tag in bytes.
const TagSize = 16

var errWriteAfterSum = errors.New("checksum already computed - adding more data is not allowed")

// Verify returns true if and only if the mac is a valid authenticator
// for msg with the given key.
func Verify(mac *[TagSize]byte, msg []byte, key [32]byte) bool {
	sum := Sum(msg, key)
	return subtle.ConstantTimeCompare(sum[:], mac[:]) == 1
}
