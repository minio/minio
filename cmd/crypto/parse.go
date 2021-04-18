// Copyright (c) 2015-2021 MinIO, Inc.
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

package crypto

import (
	"encoding/hex"
	"strings"
)

// ParseMasterKey parses the value of the environment variable
// `EnvKMSMasterKey` and returns a key-ID and a master-key KMS on success.
func ParseMasterKey(envArg string) (KMS, error) {
	values := strings.SplitN(envArg, ":", 2)
	if len(values) != 2 {
		return nil, Errorf("Invalid KMS master key: %s does not contain a ':'", envArg)
	}
	var (
		keyID  = values[0]
		hexKey = values[1]
	)
	if len(hexKey) != 64 { // 2 hex bytes = 1 byte
		return nil, Errorf("Invalid KMS master key: %s not a 32 bytes long HEX value", hexKey)
	}
	var masterKey [32]byte
	if _, err := hex.Decode(masterKey[:], []byte(hexKey)); err != nil {
		return nil, Errorf("Invalid KMS master key: %v", err)
	}
	return NewMasterKey(keyID, masterKey), nil
}
