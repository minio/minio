// Minio Cloud Storage, (C) 2019 Minio, Inc.
//
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
