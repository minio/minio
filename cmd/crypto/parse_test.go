// MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

import "testing"

func TestParseMasterKey(t *testing.T) {
	tests := []struct {
		envValue      string
		expectedKeyID string
		success       bool
	}{
		{
			envValue: "invalid-value",
			success:  false,
		},
		{
			envValue: "too:many:colons",
			success:  false,
		},
		{
			envValue: "myminio-key:not-a-hex",
			success:  false,
		},
		{
			envValue:      "my-minio-key:6368616e676520746869732070617373776f726420746f206120736563726574",
			expectedKeyID: "my-minio-key",
			success:       true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.envValue, func(t *testing.T) {
			kms, err := ParseMasterKey(tt.envValue)
			if tt.success && err != nil {
				t.Error(err)
			}
			if !tt.success && err == nil {
				t.Error("Unexpected failure")
			}
			if err == nil && kms.DefaultKeyID() != tt.expectedKeyID {
				t.Errorf("Expected keyID %s, got %s", tt.expectedKeyID, kms.DefaultKeyID())
			}
		})
	}
}
