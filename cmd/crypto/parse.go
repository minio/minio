// MinIO Cloud Storage, (C) 2017-2019 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
