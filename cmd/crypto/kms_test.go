// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
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
	"bytes"
	"path"
	"strings"
	"testing"
)

var masterKeyKMSTests = []struct {
	GenKeyID, UnsealKeyID     string
	GenContext, UnsealContext Context

	ShouldFail bool
}{
	{GenKeyID: "", UnsealKeyID: "", GenContext: Context{}, UnsealContext: nil, ShouldFail: false},                                                                                     // 0
	{GenKeyID: "ac47be7f", UnsealKeyID: "ac47be7f", GenContext: Context{}, UnsealContext: Context{}, ShouldFail: false},                                                               // 1
	{GenKeyID: "ac47be7f", UnsealKeyID: "ac47be7f", GenContext: Context{"bucket": "object"}, UnsealContext: Context{"bucket": "object"}, ShouldFail: false},                           // 2
	{GenKeyID: "", UnsealKeyID: "", GenContext: Context{"bucket": path.Join("bucket", "object")}, UnsealContext: Context{"bucket": path.Join("bucket", "object")}, ShouldFail: false}, // 3
	{GenKeyID: "", UnsealKeyID: "", GenContext: Context{"a": "a", "0": "0", "b": "b"}, UnsealContext: Context{"b": "b", "a": "a", "0": "0"}, ShouldFail: false},                       // 4

	{GenKeyID: "ac47be7f", UnsealKeyID: "ac47be7e", GenContext: Context{}, UnsealContext: Context{}, ShouldFail: true},                                                               // 5
	{GenKeyID: "ac47be7f", UnsealKeyID: "ac47be7f", GenContext: Context{"bucket": "object"}, UnsealContext: Context{"Bucket": "object"}, ShouldFail: true},                           // 6
	{GenKeyID: "", UnsealKeyID: "", GenContext: Context{"bucket": path.Join("bucket", "Object")}, UnsealContext: Context{"bucket": path.Join("bucket", "object")}, ShouldFail: true}, // 7
	{GenKeyID: "", UnsealKeyID: "", GenContext: Context{"a": "a", "0": "1", "b": "b"}, UnsealContext: Context{"b": "b", "a": "a", "0": "0"}, ShouldFail: true},                       // 8
}

func TestMasterKeyKMS(t *testing.T) {
	kms := NewKMS([32]byte{})
	for i, test := range masterKeyKMSTests {
		key, sealedKey, err := kms.GenerateKey(test.GenKeyID, test.GenContext)
		if err != nil {
			t.Errorf("Test %d: KMS failed to generate key: %v", i, err)
		}
		unsealedKey, err := kms.UnsealKey(test.UnsealKeyID, sealedKey, test.UnsealContext)
		if err != nil && !test.ShouldFail {
			t.Errorf("Test %d: KMS failed to unseal the generated key: %v", i, err)
		}
		if err == nil && test.ShouldFail {
			t.Errorf("Test %d: KMS unsealed the generated successfully but should have failed", i)
		}
		if !test.ShouldFail && !bytes.Equal(key, unsealedKey) {
			t.Errorf("Test %d: The generated and unsealed key differ", i)
		}
	}
}

var contextWriteToTests = []struct {
	Context      Context
	ExpectedJSON string
}{
	{Context: Context{}, ExpectedJSON: "{}"},                                                    // 0
	{Context: Context{"a": "b"}, ExpectedJSON: `{"a":"b"}`},                                     // 1
	{Context: Context{"a": "b", "c": "d"}, ExpectedJSON: `{"a":"b","c":"d"}`},                   // 2
	{Context: Context{"c": "d", "a": "b"}, ExpectedJSON: `{"a":"b","c":"d"}`},                   // 3
	{Context: Context{"0": "1", "-": "2", ".": "#"}, ExpectedJSON: `{"-":"2",".":"#","0":"1"}`}, // 4
}

func TestContextWriteTo(t *testing.T) {
	for i, test := range contextWriteToTests {
		var jsonContext strings.Builder
		if _, err := test.Context.WriteTo(&jsonContext); err != nil {
			t.Errorf("Test %d: Failed to encode context: %v", i, err)
			continue
		}
		if s := jsonContext.String(); s != test.ExpectedJSON {
			t.Errorf("Test %d: JSON representation differ - got: '%s' want: '%s'", i, s, test.ExpectedJSON)
		}
	}
}
