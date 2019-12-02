// Minio Cloud Storage, (C) 2019 Minio, Inc.
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
	"testing"
)

var verifyVaultConfigTests = []struct {
	Config     VaultConfig
	ShouldFail bool
}{
	{
		ShouldFail: true,
		Config: VaultConfig{
			Endpoint: "https://127.0.0.1:8080",
			Enabled:  true,
		},
	},
	{
		ShouldFail: true, // 1
		Config: VaultConfig{
			Enabled:  true,
			Endpoint: "https://127.0.0.1:8080",
			Auth:     VaultAuth{Type: "unsupported"},
		},
	},
	{
		ShouldFail: true, // 2
		Config: VaultConfig{
			Enabled:  true,
			Endpoint: "https://127.0.0.1:8080",
			Auth: VaultAuth{
				Type:    "approle",
				AppRole: VaultAppRole{},
			},
		},
	},
	{
		ShouldFail: true, // 3
		Config: VaultConfig{
			Enabled:  true,
			Endpoint: "https://127.0.0.1:8080",
			Auth: VaultAuth{
				Type:    "approle",
				AppRole: VaultAppRole{ID: "123456"},
			},
		},
	},
	{
		ShouldFail: true, // 4
		Config: VaultConfig{
			Enabled:  true,
			Endpoint: "https://127.0.0.1:8080",
			Auth: VaultAuth{
				Type:    "approle",
				AppRole: VaultAppRole{ID: "123456", Secret: "abcdef"},
			},
		},
	},
	{
		ShouldFail: true, // 5
		Config: VaultConfig{
			Enabled:  true,
			Endpoint: "https://127.0.0.1:8080",
			Auth: VaultAuth{
				Type:    "approle",
				AppRole: VaultAppRole{ID: "123456", Secret: "abcdef"},
			},
			Key: VaultKey{Name: "default-key", Version: -1},
		},
	},
}

func TestVerifyVaultConfig(t *testing.T) {
	for _, test := range verifyVaultConfigTests {
		test := test
		t.Run(test.Config.Endpoint, func(t *testing.T) {
			err := test.Config.Verify()
			if test.ShouldFail && err == nil {
				t.Errorf("Verify should fail but returned 'err == nil'")
			}
			if !test.ShouldFail && err != nil {
				t.Errorf("Verify should succeed but returned err: %s", err)
			}
		})
	}
}
