/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
 *
 */

package madmin

import (
	"encoding/json"
	"testing"
)

// TestUnmarshalInvalidTierConfig tests that TierConfig parsing can catch invalid tier configs
func TestUnmarshalInvalidTierConfig(t *testing.T) {
	testCases := []struct {
		cfg TierConfig
		err error
	}{
		{
			cfg: TierConfig{
				Version: TierConfigV1,
				Name:    "S3TIER?",
				Type:    S3,
				GCS: &TierGCS{
					Creds:        "VWJ1bnR1IDIwLjA0LjEgTFRTIFxuIFxsCgo",
					Bucket:       "ilmtesting",
					Endpoint:     "https://storage.googleapis.com/",
					Prefix:       "testprefix",
					Region:       "us-west-2",
					StorageClass: "",
				},
			},
			err: ErrTierInvalidConfig,
		},
		{
			cfg: TierConfig{
				Version: "invalid-version",
				Name:    "INVALIDTIER",
				Type:    GCS,
				GCS: &TierGCS{
					Creds:        "VWJ1bnR1IDIwLjA0LjEgTFRTIFxuIFxsCgo",
					Bucket:       "ilmtesting",
					Endpoint:     "https://storage.googleapis.com/",
					Prefix:       "testprefix",
					Region:       "us-west-2",
					StorageClass: "",
				},
			},
			err: ErrTierInvalidConfigVersion,
		},
		{
			cfg: TierConfig{
				Version: TierConfigV1,
				Type:    GCS,
				GCS: &TierGCS{
					Creds:        "VWJ1bnR1IDIwLjA0LjEgTFRTIFxuIFxsCgo",
					Bucket:       "ilmtesting",
					Endpoint:     "https://storage.googleapis.com/",
					Prefix:       "testprefix",
					Region:       "us-west-2",
					StorageClass: "",
				},
			},
			err: ErrTierNameEmpty,
		},
		{
			cfg: TierConfig{
				Version: TierConfigV1,
				Name:    "GCSTIER",
				Type:    GCS,
				GCS: &TierGCS{
					Creds:        "VWJ1bnR1IDIwLjA0LjEgTFRTIFxuIFxsCgo",
					Bucket:       "ilmtesting",
					Endpoint:     "https://storage.googleapis.com/",
					Prefix:       "testprefix",
					Region:       "us-west-2",
					StorageClass: "",
				},
			},
			err: nil,
		},
	}
	for i, tc := range testCases {
		data, err := json.Marshal(tc.cfg)
		if err != nil {
			t.Fatalf("Test %d: Failed to marshal tier config %v: %v", i+1, tc.cfg, err)
		}
		var cfg TierConfig
		err = json.Unmarshal(data, &cfg)
		if err != tc.err {
			t.Fatalf("Test %d: Failed in unmarshal tier config %s: expected %v got %v", i+1, data, tc.err, err)
		}
	}

	// Test invalid tier type
	evilJSON := []byte(`{
                             "Version": "v1",
                             "Type" : "not-a-type",
                             "Name" : "GCSTIER3",
                             "GCS" : {
                               "Bucket" : "ilmtesting",
                               "Prefix" : "testprefix3",
                               "Endpoint" : "https://storage.googleapis.com/",
                               "Creds": "VWJ1bnR1IDIwLjA0LjEgTFRTIFxuIFxsCgo",
                               "Region" : "us-west-2",
                               "StorageClass" : ""
                             }
                            }`)
	var cfg TierConfig
	err := json.Unmarshal(evilJSON, &cfg)
	if err != ErrTierTypeUnsupported {
		t.Fatalf("Expected to fail with unsupported type but got %v", err)
	}
}
