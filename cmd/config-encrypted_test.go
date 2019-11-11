/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"testing"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
)

func TestDecryptData(t *testing.T) {
	cred1 := auth.Credentials{
		AccessKey: "minio",
		SecretKey: "minio123",
	}

	cred2 := auth.Credentials{
		AccessKey: "minio",
		SecretKey: "minio1234",
	}

	data := []byte(`config data`)
	edata1, err := madmin.EncryptData(cred1.String(), data)
	if err != nil {
		t.Fatal(err)
	}

	edata2, err := madmin.EncryptData(cred2.String(), data)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		edata   []byte
		creds   []auth.Credentials
		success bool
	}{
		{edata1, []auth.Credentials{cred1, cred2}, true},
		{edata2, []auth.Credentials{cred1, cred2}, true},
		{data, []auth.Credentials{cred1, cred2}, false},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ddata, err := decryptData(test.edata, test.creds...)
			if err != nil && test.success {
				t.Errorf("Expected success, saw failure %v", err)
			}
			if err == nil && !test.success {
				t.Error("Expected failure, saw success")
			}
			if test.success {
				if !bytes.Equal(ddata, data) {
					t.Errorf("Expected %s, got %s", string(data), string(ddata))
				}
			}
		})
	}
}
