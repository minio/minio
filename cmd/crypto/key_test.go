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
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"
)

var shortRandom = func(limit int64) io.Reader { return io.LimitReader(rand.Reader, limit) }

func recoverTest(i int, shouldPass bool, t *testing.T) {
	if err := recover(); err == nil && !shouldPass {
		t.Errorf("Test %d should fail but passed successfully", i)
	} else if err != nil && shouldPass {
		t.Errorf("Test %d should pass but failed: %v", i, err)
	}
}

var generateKeyTests = []struct {
	ExtKey     [32]byte
	Random     io.Reader
	ShouldPass bool
}{
	{ExtKey: [32]byte{}, Random: nil, ShouldPass: true},             // 0
	{ExtKey: [32]byte{}, Random: rand.Reader, ShouldPass: true},     // 1
	{ExtKey: [32]byte{}, Random: shortRandom(32), ShouldPass: true}, // 2
	// {ExtKey: [32]byte{}, Random: shortRandom(31), ShouldPass: false}, // 3 See: https://github.com/minio/minio/issues/6064
}

func TestGenerateKey(t *testing.T) {
	for i, test := range generateKeyTests {
		func() {
			defer recoverTest(i, test.ShouldPass, t)
			key := GenerateKey(test.ExtKey, test.Random)
			if [32]byte(key) == [32]byte{} {
				t.Errorf("Test %d: generated key is zero key", i) // check that we generate random and unique key
			}
		}()
	}
}

var sealUnsealKeyTests = []struct {
	SealExtKey, SealIV     [32]byte
	SealBucket, SealObject string

	UnsealExtKey, UnsealIV     [32]byte
	UnsealBucket, UnsealObject string

	ShouldPass bool
}{
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{}, UnsealIV: [32]byte{}, UnsealBucket: "bucket", UnsealObject: "object",
		ShouldPass: true,
	}, // 0
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{1}, UnsealIV: [32]byte{0}, UnsealBucket: "bucket", UnsealObject: "object",
		ShouldPass: false,
	}, // 1
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{}, UnsealIV: [32]byte{1}, UnsealBucket: "bucket", UnsealObject: "object",
		ShouldPass: false,
	}, // 2
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{}, UnsealIV: [32]byte{}, UnsealBucket: "Bucket", UnsealObject: "object",
		ShouldPass: false,
	}, // 3
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{}, UnsealIV: [32]byte{}, UnsealBucket: "bucket", UnsealObject: "Object",
		ShouldPass: false,
	}, // 4
}

func TestSealUnsealKey(t *testing.T) {
	for i, test := range sealUnsealKeyTests {
		key := GenerateKey(test.SealExtKey, rand.Reader)
		sealedKey := key.Seal(test.SealExtKey, test.SealIV, test.SealBucket, test.SealObject)
		if err := key.Unseal(sealedKey, test.UnsealExtKey, test.UnsealIV, test.UnsealBucket, test.UnsealObject); err == nil && !test.ShouldPass {
			t.Errorf("Test %d should fail but passed successfully", i)
		} else if err != nil && test.ShouldPass {
			t.Errorf("Test %d should pass put failed: %v", i, err)
		}
	}
}

var derivePartKeyTest = []struct {
	PartID  uint32
	PartKey string
}{
	{PartID: 0, PartKey: "aa7855e13839dd767cd5da7c1ff5036540c9264b7a803029315e55375287b4af"},
	{PartID: 1, PartKey: "a3e7181c6eed030fd52f79537c56c4d07da92e56d374ff1dd2043350785b37d8"},
	{PartID: 10000, PartKey: "f86e65c396ed52d204ee44bd1a0bbd86eb8b01b7354e67a3b3ae0e34dd5bd115"},
}

func TestDerivePartKey(t *testing.T) {
	var key ObjectKey
	for i, test := range derivePartKeyTest {
		expectedPartKey, err := hex.DecodeString(test.PartKey)
		if err != nil {
			t.Fatalf("Test %d failed to decode expected part-key: %v", i, err)
		}
		partKey := key.DerivePartKey(test.PartID)
		if !bytes.Equal(partKey[:], expectedPartKey[:]) {
			t.Errorf("Test %d derives wrong part-key: got '%s' want: '%s'", i, hex.EncodeToString(partKey[:]), test.PartKey)
		}
	}
}
