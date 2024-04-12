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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/secure-io/sio-go"
)

func extractInspectV1(keyHex string, r io.Reader, w io.Writer, okMsg string) error {
	id, err := hex.DecodeString(keyHex[:8])
	if err != nil {
		return err
	}
	key, err := hex.DecodeString(keyHex[8:])
	if err != nil {
		return err
	}
	// Verify that CRC is ok.
	want := binary.LittleEndian.Uint32(id)
	got := crc32.ChecksumIEEE(key)
	if want != got {
		return fmt.Errorf("Invalid key checksum, want %x, got %x", want, got)
	}

	stream, err := sio.AES_256_GCM.Stream(key)
	if err != nil {
		return err
	}
	// Zero nonce, we only use each key once, and 32 bytes is plenty.
	nonce := make([]byte, stream.NonceSize())
	encr := stream.DecryptReader(r, nonce, nil)
	_, err = io.Copy(w, encr)
	if err == nil {
		fmt.Println(okMsg)
	}
	return err
}
