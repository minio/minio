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
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"

	"github.com/minio/minio/internal/logger"
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
	{ExtKey: [32]byte{}, Random: nil, ShouldPass: true},              // 0
	{ExtKey: [32]byte{}, Random: rand.Reader, ShouldPass: true},      // 1
	{ExtKey: [32]byte{}, Random: shortRandom(32), ShouldPass: true},  // 2
	{ExtKey: [32]byte{}, Random: shortRandom(31), ShouldPass: false}, // 3
}

func TestGenerateKey(t *testing.T) {
	defer func(l bool) { logger.DisableLog = l }(logger.DisableLog)
	logger.DisableLog = true

	for i, test := range generateKeyTests {
		i, test := i, test
		func() {
			defer recoverTest(i, test.ShouldPass, t)
			key := GenerateKey(test.ExtKey[:], test.Random)
			if [32]byte(key) == [32]byte{} {
				t.Errorf("Test %d: generated key is zero key", i) // check that we generate random and unique key
			}
		}()
	}
}

var generateIVTests = []struct {
	Random     io.Reader
	ShouldPass bool
}{
	{Random: nil, ShouldPass: true},              // 0
	{Random: rand.Reader, ShouldPass: true},      // 1
	{Random: shortRandom(32), ShouldPass: true},  // 2
	{Random: shortRandom(31), ShouldPass: false}, // 3
}

func TestGenerateIV(t *testing.T) {
	defer func(l bool) { logger.DisableLog = l }(logger.DisableLog)
	logger.DisableLog = true

	for i, test := range generateIVTests {
		i, test := i, test
		func() {
			defer recoverTest(i, test.ShouldPass, t)
			iv := GenerateIV(test.Random)
			if iv == [32]byte{} {
				t.Errorf("Test %d: generated IV is zero IV", i) // check that we generate random and unique IV
			}
		}()
	}
}

var sealUnsealKeyTests = []struct {
	SealExtKey, SealIV                 [32]byte
	SealDomain, SealBucket, SealObject string

	UnsealExtKey                             [32]byte
	UnsealDomain, UnsealBucket, UnsealObject string

	ShouldPass bool
}{
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealDomain: "SSE-C", SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{}, UnsealDomain: "SSE-C", UnsealBucket: "bucket", UnsealObject: "object",
		ShouldPass: true,
	}, // 0
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealDomain: "SSE-C", SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{1}, UnsealDomain: "SSE-C", UnsealBucket: "bucket", UnsealObject: "object", // different ext-key
		ShouldPass: false,
	}, // 1
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealDomain: "SSE-S3", SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{}, UnsealDomain: "SSE-C", UnsealBucket: "bucket", UnsealObject: "object", // different domain
		ShouldPass: false,
	}, // 2
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealDomain: "SSE-C", SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{}, UnsealDomain: "SSE-C", UnsealBucket: "Bucket", UnsealObject: "object", // different bucket
		ShouldPass: false,
	}, // 3
	{
		SealExtKey: [32]byte{}, SealIV: [32]byte{}, SealDomain: "SSE-C", SealBucket: "bucket", SealObject: "object",
		UnsealExtKey: [32]byte{}, UnsealDomain: "SSE-C", UnsealBucket: "bucket", UnsealObject: "Object", // different object
		ShouldPass: false,
	}, // 4
}

func TestSealUnsealKey(t *testing.T) {
	for i, test := range sealUnsealKeyTests {
		key := GenerateKey(test.SealExtKey[:], rand.Reader)
		sealedKey := key.Seal(test.SealExtKey[:], test.SealIV, test.SealDomain, test.SealBucket, test.SealObject)
		if err := key.Unseal(test.UnsealExtKey[:], sealedKey, test.UnsealDomain, test.UnsealBucket, test.UnsealObject); err == nil && !test.ShouldPass {
			t.Errorf("Test %d should fail but passed successfully", i)
		} else if err != nil && test.ShouldPass {
			t.Errorf("Test %d should pass put failed: %v", i, err)
		}
	}

	// Test legacy InsecureSealAlgorithm
	var extKey, iv [32]byte
	key := GenerateKey(extKey[:], rand.Reader)
	sealedKey := key.Seal(extKey[:], iv, "SSE-S3", "bucket", "object")
	sealedKey.Algorithm = InsecureSealAlgorithm
	if err := key.Unseal(extKey[:], sealedKey, "SSE-S3", "bucket", "object"); err == nil {
		t.Errorf("'%s' test succeeded but it should fail because the legacy algorithm was used", sealedKey.Algorithm)
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
		if !bytes.Equal(partKey[:], expectedPartKey) {
			t.Errorf("Test %d derives wrong part-key: got '%s' want: '%s'", i, hex.EncodeToString(partKey[:]), test.PartKey)
		}
	}
}

var sealUnsealETagTests = []string{
	"",
	"90682b8e8cc7609c",
	"90682b8e8cc7609c4671e1d64c73fc30",
	"90682b8e8cc7609c4671e1d64c73fc307fb3104f",
}

func TestSealETag(t *testing.T) {
	var key ObjectKey
	for i := range key {
		key[i] = byte(i)
	}
	for i, etag := range sealUnsealETagTests {
		tag, err := hex.DecodeString(etag)
		if err != nil {
			t.Errorf("Test %d: failed to decode etag: %s", i, err)
		}
		sealedETag := key.SealETag(tag)
		unsealedETag, err := key.UnsealETag(sealedETag)
		if err != nil {
			t.Errorf("Test %d: failed to decrypt etag: %s", i, err)
		}
		if !bytes.Equal(unsealedETag, tag) {
			t.Errorf("Test %d: unsealed etag does not match: got %s - want %s", i, hex.EncodeToString(unsealedETag), etag)
		}
	}
}
