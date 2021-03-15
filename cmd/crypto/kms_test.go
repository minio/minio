// MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
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
	"encoding/base64"
	"fmt"
	"path"
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
	for i, test := range masterKeyKMSTests {
		kms := NewMasterKey(test.GenKeyID, [32]byte{})

		key, sealedKey, err := kms.GenerateKey(test.GenKeyID, test.GenContext)
		if err != nil {
			t.Errorf("Test %d: KMS failed to generate key: %v", i, err)
		}
		unsealedKey, err := kms.UnsealKey(test.UnsealKeyID, sealedKey, test.UnsealContext)
		if err != nil && !test.ShouldFail {
			t.Errorf("Test %d: KMS failed to unseal the generated key: %v", i, err)
		}
		if err == nil && test.ShouldFail {
			t.Errorf("Test %d: KMS unsealed the generated key successfully but should have failed", i)
		}
		if !test.ShouldFail && !bytes.Equal(key[:], unsealedKey[:]) {
			t.Errorf("Test %d: The generated and unsealed key differ", i)
		}
	}
}

var masterKeyCompatibleWithKESTests = []struct {
	SealedKey string
	Context   Context
	Plaintext string
}{
	{
		SealedKey: "eyJhZWFkIjoiQ2hhQ2hhMjBQb2x5MTMwNSIsIml2IjoiVjQ1SGV1Vm0rb0RrYUdjdkY3dU9LZz09Iiwibm9uY2UiOiJhYkFZL2M2OGEwU3RKbHdGIiwiYnl0ZXMiOiJUbktBOXhpWmlmaS9obE5hMENzZjM2b3JBTTJKaitRU2g4eCtKV3lqajNKZEVzK2hLbjM1dXljTEhLcU9GSG45In0=",
		Plaintext: "pDdhxcXGiMscyy0mlFVU+gG8nREO8nsyjjG+q1j3ltg=",
	},
	{
		SealedKey: "eyJhZWFkIjoiQ2hhQ2hhMjBQb2x5MTMwNSIsIml2IjoiS0lVcmEwdklRUmlDd0tVWXVWcUFUUT09Iiwibm9uY2UiOiJnY1F3ZHBNazREbUE0M1NLIiwiYnl0ZXMiOiJTOUV4cVBXL2FGL3E1SE1SdE5samRaQ2pTWUhZQUhhZHYwS2t4NGpRa2tIbzd0cy9QNWllVVE1OTVNRTZxSHFSIn0=",
		Context:   Context{"Hello": "World"},
		Plaintext: "BvRPJq5vMoFmnHqOfHoZr+pxDlmW1US9bFczlLglj4g=",
	},
	{
		SealedKey: "eyJhZWFkIjoiQ2hhQ2hhMjBQb2x5MTMwNSIsIml2IjoiMnpkaVJOWGZ1ZDE0bTh5SzhtRlZMQT09Iiwibm9uY2UiOiJFcXhVQm0zZnQ1clpxKzJtIiwiYnl0ZXMiOiI3aTBqK1VzVnU0U2ZvdlUrS28zWFloNFpLaG1qaFdPVFltM3ExTHVQRDlHZUQrZ2VhN0kzWm94R1pnOWdZdXlYIn0=",
		Context:   Context{"bucket": "object", "Hello": "World"},
		Plaintext: "dZ0aKnlDIbgcH/+4/dmfgNq5mY4Vb0b43zWkVqBRE5M=",
	},
}

func TestMasterKeyCompatibleWithKES(t *testing.T) {
	kms, err := ParseMasterKey("my-key:5vXJlAUre5g+5K1zmENckYpCjiWQNvQLhBz5IC0bF4A=")
	if err != nil {
		t.Fatalf("Failed to create KMS from master key: %v", err)
	}

	for i, test := range masterKeyCompatibleWithKESTests {
		sealedKey, err := base64.StdEncoding.DecodeString(test.SealedKey)
		if err != nil {
			t.Fatalf("Test %d: Failed to decode sealed key: %v", i, err)
		}
		plaintext, err := base64.StdEncoding.DecodeString(test.Plaintext)
		if err != nil {
			t.Fatalf("Test %d: Failed to decode plaintext: %v", i, err)
		}

		key, err := kms.UnsealKey("my-key", sealedKey, test.Context)
		if err != nil {
			t.Fatalf("Test %d: Failed to decrypt sealed key: %v", i, err)
		}
		if !bytes.Equal(plaintext, key[:]) {
			t.Fatalf("Test %d: Plaintext mismatch: got '%x' - want '%x'", i, key[:], plaintext)
		}
	}
}

var contextMarshalTextTests = []struct {
	Context      Context
	ExpectedJSON string
}{
	0: {Context: Context{}, ExpectedJSON: "{}"},
	1: {Context: Context{"a": "b"}, ExpectedJSON: `{"a":"b"}`},
	2: {Context: Context{"a": "b", "c": "d"}, ExpectedJSON: `{"a":"b","c":"d"}`},
	3: {Context: Context{"c": "d", "a": "b"}, ExpectedJSON: `{"a":"b","c":"d"}`},
	4: {Context: Context{"0": "1", "-": "2", ".": "#"}, ExpectedJSON: `{"-":"2",".":"#","0":"1"}`},
	// rfc 8259 escapes
	5: {Context: Context{"0": "1", "key\\": "val\tue\r\n", "\"": "\""}, ExpectedJSON: `{"\"":"\"","0":"1","key\\":"val\tue\r\n"}`},
	// html sensitive escapes
	6: {Context: Context{"a": "<>&"}, ExpectedJSON: `{"a":"\u003c\u003e\u0026"}`},
}

func TestContextMarshalText(t *testing.T) {
	for i, test := range contextMarshalTextTests {
		text, err := test.Context.MarshalText()
		if err != nil {
			t.Fatalf("Test %d: Failed to encode context: %v", i, err)
		}
		if string(text) != test.ExpectedJSON {
			t.Errorf("Test %d: JSON representation differ - got: '%s' want: '%s'", i, string(text), test.ExpectedJSON)
		}
	}
}

func BenchmarkContext(b *testing.B) {
	tests := []Context{{}, {"bucket": "warp-benchmark-bucket"}, {"0": "1", "-": "2", ".": "#"}, {"34trg": "dfioutr89", "ikjfdghkjf": "jkedfhgfjkhg", "sdfhsdjkh": "if88889", "asddsirfh804": "kjfdshgdfuhgfg78-45604586#$%<>&"}}
	for _, test := range tests {
		b.Run(fmt.Sprintf("%d-elems", len(test)), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := test.MarshalText()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
