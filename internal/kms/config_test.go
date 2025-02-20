// Copyright (c) 2015-2024 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
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

package kms

import (
	"os"
	"testing"
)

func TestIsPresent(t *testing.T) {
	for i, test := range isPresentTests {
		os.Clearenv()
		for k, v := range test.Env {
			t.Setenv(k, v)
		}

		ok, err := IsPresent()
		if err != nil && !test.ShouldFail {
			t.Fatalf("Test %d: %v", i, err)
		}
		if err == nil && test.ShouldFail {
			t.Fatalf("Test %d: should have failed but succeeded", i)
		}

		if !test.ShouldFail && ok != test.IsPresent {
			t.Fatalf("Test %d: reported that KMS present=%v - want present=%v", i, ok, test.IsPresent)
		}
	}
}

var isPresentTests = []struct {
	Env        map[string]string
	IsPresent  bool
	ShouldFail bool
}{
	{Env: map[string]string{}}, // 0
	{ // 1
		Env: map[string]string{
			EnvKMSSecretKey: "minioy-default-key:6jEQjjMh8iPq8/gqgb4eMDIZFOtPACIsr9kO+vx8JFs=",
		},
		IsPresent: true,
	},
	{ // 2
		Env: map[string]string{
			EnvKMSEndpoint:   "https://127.0.0.1:7373",
			EnvKMSDefaultKey: "minio-key",
			EnvKMSEnclave:    "demo",
			EnvKMSAPIKey:     "k1:MBDtmC9ZAf3Wi4-oGglgKx_6T1jwJfct1IC15HOxetg",
		},
		IsPresent: true,
	},
	{ // 3
		Env: map[string]string{
			EnvKESEndpoint:   "https://127.0.0.1:7373",
			EnvKESDefaultKey: "minio-key",
			EnvKESAPIKey:     "kes:v1:AGtR4PvKXNjz+/MlBX2Djg0qxwS3C4OjoDzsuFSQr82e",
		},
		IsPresent: true,
	},
	{ // 4
		Env: map[string]string{
			EnvKESEndpoint:   "https://127.0.0.1:7373",
			EnvKESDefaultKey: "minio-key",
			EnvKESClientKey:  "/tmp/client.key",
			EnvKESClientCert: "/tmp/client.crt",
		},
		IsPresent: true,
	},
	{ // 5
		Env: map[string]string{
			EnvKMSEndpoint: "https://127.0.0.1:7373",
			EnvKESEndpoint: "https://127.0.0.1:7373",
		},
		ShouldFail: true,
	},
	{ // 6
		Env: map[string]string{
			EnvKMSEndpoint:  "https://127.0.0.1:7373",
			EnvKMSSecretKey: "minioy-default-key:6jEQjjMh8iPq8/gqgb4eMDIZFOtPACIsr9kO+vx8JFs=",
		},
		ShouldFail: true,
	},
	{ // 7
		Env: map[string]string{
			EnvKMSEnclave:  "foo",
			EnvKESServerCA: "/etc/minio/certs",
		},
		ShouldFail: true,
	},
}
