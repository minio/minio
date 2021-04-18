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
	"bytes"
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

		key, err := kms.GenerateKey(test.GenKeyID, test.GenContext)
		if err != nil {
			t.Errorf("Test %d: KMS failed to generate key: %v", i, err)
		}
		unsealedKey, err := kms.DecryptKey(test.UnsealKeyID, key.Ciphertext, test.UnsealContext)
		if err != nil && !test.ShouldFail {
			t.Errorf("Test %d: KMS failed to unseal the generated key: %v", i, err)
		}
		if err == nil && test.ShouldFail {
			t.Errorf("Test %d: KMS unsealed the generated key successfully but should have failed", i)
		}
		if !test.ShouldFail && !bytes.Equal(key.Plaintext, unsealedKey[:]) {
			t.Errorf("Test %d: The generated and unsealed key differ", i)
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
