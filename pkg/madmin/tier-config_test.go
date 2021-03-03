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
	evilJSON := []byte(`{
      "Version": "v1",
      "Type" : "azure",
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
	var tier TierConfig
	err := json.Unmarshal(evilJSON, &tier)
	if err == nil {
		t.Fatalf("expected to fail but got %v", tier)
	}
}
