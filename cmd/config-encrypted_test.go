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

package cmd

import (
	"bytes"
	"testing"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/auth"
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
		cred    auth.Credentials
		success bool
	}{
		{edata1, cred1, true},
		{edata2, cred2, true},
		{data, cred1, false},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			ddata, err := madmin.DecryptData(test.cred.String(), bytes.NewReader(test.edata))
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
