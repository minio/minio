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
	"encoding/base64"
	"strings"
)

// ParseMasterKey parses a KMS master key that has been specified
// as env. variable and returns KMS on success.
//
// The KMS uses key specified as env. variable to generate new and
// decrypt existing data encryption keys. The keys prodcued by this
// KMS are compatible to keys produced by KES.
func ParseMasterKey(envArg string) (KMS, error) {
	values := strings.SplitN(envArg, ":", 2)
	if len(values) != 2 {
		return nil, Errorf("Invalid KMS master key: %s does not contain a ':'", envArg)
	}
	var (
		keyID  = values[0]
		b64Key = values[1]
	)
	b, err := base64.StdEncoding.DecodeString(b64Key)
	if err != nil {
		return nil, Errorf("Invalid KMS master key: %v", err)
	}
	if len(b) != 32 {
		return nil, Errorf("Invalid KMS master key: %s not a 32 bytes long base64 value", b64Key)
	}

	var masterKey [32]byte
	copy(masterKey[:], b)
	return NewMasterKey(keyID, masterKey), nil
}
