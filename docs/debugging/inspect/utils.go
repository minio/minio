// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"log"
)

func bytesToPrivateKey(priv []byte) (*rsa.PrivateKey, error) {
	// Try PEM
	if block, _ := pem.Decode(priv); block != nil {
		return x509.ParsePKCS1PrivateKey(block.Bytes)
	}
	// Try base 64
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(priv)))
	if n, err := base64.StdEncoding.Decode(dst, priv); err == nil {
		return x509.ParsePKCS1PrivateKey(dst[:n])
	}
	// Try Raw, return error
	return x509.ParsePKCS1PrivateKey(priv)
}

func fatalErr(err error) {
	if err == nil {
		return
	}
	log.Fatalln(err)
}

func fatalIf(b bool, msg string, v ...interface{}) {
	if !b {
		return
	}
	log.Fatalf(msg, v...)
}
