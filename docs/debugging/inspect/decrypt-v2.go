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
	"archive/zip"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"io"
	"io/ioutil"
	"os"

	"github.com/secure-io/sio-go"
)

func extractInspectV2(privKeyPath string, r *os.File, w io.Writer) error {
	privKeyBytes, err := ioutil.ReadFile(privKeyPath)
	if err != nil {
		return err
	}
	privKey, err := bytesToPrivateKey(privKeyBytes)
	if err != nil {
		return err
	}

	st, err := r.Stat()
	if err != nil {
		return err
	}

	archive, err := zip.NewReader(r, st.Size())
	if err != nil {
		return err
	}

	var key []byte

	// Extract key from key.enc inside the archive
	for _, f := range archive.File {
		if f.Name == "key.enc" {
			zr, err := f.Open()
			if err != nil {
				return err
			}
			cipherKeyBuf := bytes.NewBuffer([]byte{})
			if _, err := io.Copy(cipherKeyBuf, zr); err != nil {
				zr.Close()
				return err
			}
			zr.Close()
			if k, err := rsa.DecryptOAEP(sha512.New(), rand.Reader, privKey, cipherKeyBuf.Bytes(), nil); err != nil {
				return err
			} else {
				key = k
			}
		}
	}

	for _, f := range archive.File {
		if f.Name == "data.zip" {
			zd, err := f.Open()
			if err != nil {
				return err
			}
			stream, err := sio.AES_256_GCM.Stream(key)
			if err != nil {
				return err
			}
			// Zero nonce, we only use each key once, and 32 bytes is plenty.
			nonce := make([]byte, stream.NonceSize())
			encr := stream.DecryptReader(zd, nonce, nil)
			_, err = io.Copy(w, encr)
			if err != nil {
				return err
			}

		}
	}

	return nil
}
