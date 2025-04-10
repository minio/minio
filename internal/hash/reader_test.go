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

package hash

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/minio/minio/internal/ioutil"
)

// Tests functions like Size(), MD5*(), SHA256*()
func TestHashReaderHelperMethods(t *testing.T) {
	r, err := NewReader(t.Context(), bytes.NewReader([]byte("abcd")), 4, "e2fc714c4727ee9395f324cd2e7f331f", "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", 4)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, r)
	if err != nil {
		t.Fatal(err)
	}
	md5sum := r.MD5Current()
	if hex.EncodeToString(md5sum) != "e2fc714c4727ee9395f324cd2e7f331f" {
		t.Errorf("Expected md5hex \"e2fc714c4727ee9395f324cd2e7f331f\", got %s", hex.EncodeToString(md5sum))
	}
	if r.SHA256HexString() != "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589" {
		t.Errorf("Expected sha256hex \"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589\", got %s", r.SHA256HexString())
	}
	if base64.StdEncoding.EncodeToString(md5sum) != "4vxxTEcn7pOV8yTNLn8zHw==" {
		t.Errorf("Expected md5base64 \"4vxxTEcn7pOV8yTNLn8zHw==\", got \"%s\"", base64.StdEncoding.EncodeToString(md5sum))
	}
	if r.Size() != 4 {
		t.Errorf("Expected size 4, got %d", r.Size())
	}
	if r.ActualSize() != 4 {
		t.Errorf("Expected size 4, got %d", r.ActualSize())
	}
	expectedMD5, err := hex.DecodeString("e2fc714c4727ee9395f324cd2e7f331f")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.MD5Current(), expectedMD5) {
		t.Errorf("Expected md5hex \"e2fc714c4727ee9395f324cd2e7f331f\", got %s", hex.EncodeToString(r.MD5Current()))
	}
	expectedSHA256, err := hex.DecodeString("88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.SHA256(), expectedSHA256) {
		t.Errorf("Expected md5hex \"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589\", got %s", r.SHA256HexString())
	}
}

// Tests hash reader checksum verification.
func TestHashReaderVerification(t *testing.T) {
	testCases := []struct {
		desc              string
		src               io.Reader
		size              int64
		actualSize        int64
		md5hex, sha256hex string
		err               error
	}{
		0: {
			desc:       "Success, no checksum verification provided.",
			src:        bytes.NewReader([]byte("abcd")),
			size:       4,
			actualSize: 4,
		},
		{
			desc:       "Failure md5 mismatch.",
			src:        bytes.NewReader([]byte("abcd")),
			size:       4,
			actualSize: 4,
			md5hex:     "d41d8cd98f00b204e9800998ecf8427f",
			err: BadDigest{
				"d41d8cd98f00b204e9800998ecf8427f",
				"e2fc714c4727ee9395f324cd2e7f331f",
			},
		},
		{
			desc:       "Failure sha256 mismatch.",
			src:        bytes.NewReader([]byte("abcd")),
			size:       4,
			actualSize: 4,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580",
			err: SHA256Mismatch{
				"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580",
				"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			},
		},
		{
			desc:       "Nested hash reader NewReader() should merge.",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4),
			size:       4,
			actualSize: 4,
		},
		{
			desc:       "Incorrect sha256, nested",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4),
			size:       4,
			actualSize: 4,
			sha256hex:  "50d858e0985ecc7f60418aaf0cc5ab587f42c2570a884095a9e8ccacd0f6545c",
			err: SHA256Mismatch{
				ExpectedSHA256:   "50d858e0985ecc7f60418aaf0cc5ab587f42c2570a884095a9e8ccacd0f6545c",
				CalculatedSHA256: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			},
		},
		5: {
			desc:       "Correct sha256, nested",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4),
			size:       4,
			actualSize: 4,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		},
		{
			desc:       "Correct sha256, nested, truncated",
			src:        mustReader(t, bytes.NewReader([]byte("abcd-more-stuff-to-be ignored")), 4, "", "", 4),
			size:       4,
			actualSize: -1,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			err:        ioutil.ErrOverread,
		},
		7: {
			desc:       "Correct sha256, nested, truncated, swapped",
			src:        mustReader(t, bytes.NewReader([]byte("abcd-more-stuff-to-be ignored")), 4, "", "", -1),
			size:       4,
			actualSize: -1,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			err:        ioutil.ErrOverread,
		},
		{
			desc:       "Incorrect MD5, nested",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4),
			size:       4,
			actualSize: 4,
			md5hex:     "0773da587b322af3a8718cb418a715ce",
			err: BadDigest{
				ExpectedMD5:   "0773da587b322af3a8718cb418a715ce",
				CalculatedMD5: "e2fc714c4727ee9395f324cd2e7f331f",
			},
		},
		{
			desc:       "Correct sha256, truncated",
			src:        bytes.NewReader([]byte("abcd-morethan-4-bytes")),
			size:       4,
			actualSize: 4,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			err:        ioutil.ErrOverread,
		},
		{
			desc:       "Correct MD5, nested",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4),
			size:       4,
			actualSize: 4,
			md5hex:     "e2fc714c4727ee9395f324cd2e7f331f",
		},
		{
			desc:       "Correct MD5, truncated",
			src:        bytes.NewReader([]byte("abcd-morethan-4-bytes")),
			size:       4,
			actualSize: 4,
			sha256hex:  "",
			md5hex:     "e2fc714c4727ee9395f324cd2e7f331f",
			err:        ioutil.ErrOverread,
		},
		{
			desc:       "Correct MD5, nested, truncated",
			src:        mustReader(t, bytes.NewReader([]byte("abcd-morestuff")), -1, "", "", -1),
			size:       4,
			actualSize: 4,
			md5hex:     "e2fc714c4727ee9395f324cd2e7f331f",
			err:        ioutil.ErrOverread,
		},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			r, err := NewReader(t.Context(), testCase.src, testCase.size, testCase.md5hex, testCase.sha256hex, testCase.actualSize)
			if err != nil {
				t.Fatalf("Test %q: Initializing reader failed %s", testCase.desc, err)
			}
			_, err = io.Copy(io.Discard, r)
			if err != nil {
				if testCase.err == nil {
					t.Errorf("Test %q; got unexpected error: %v", testCase.desc, err)
					return
				}
				if err.Error() != testCase.err.Error() {
					t.Errorf("Test %q: Expected error %s, got error %s", testCase.desc, testCase.err, err)
				}
			}
		})
	}
}

func mustReader(t *testing.T, src io.Reader, size int64, md5Hex, sha256Hex string, actualSize int64) *Reader {
	r, err := NewReader(t.Context(), src, size, md5Hex, sha256Hex, actualSize)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Tests NewReader() constructor with invalid arguments.
func TestHashReaderInvalidArguments(t *testing.T) {
	testCases := []struct {
		desc              string
		src               io.Reader
		size              int64
		actualSize        int64
		md5hex, sha256hex string
		success           bool
	}{
		{
			desc:       "Invalid md5sum NewReader() will fail.",
			src:        bytes.NewReader([]byte("abcd")),
			size:       4,
			actualSize: 4,
			md5hex:     "invalid-md5",
			success:    false,
		},
		{
			desc:       "Invalid sha256 NewReader() will fail.",
			src:        bytes.NewReader([]byte("abcd")),
			size:       4,
			actualSize: 4,
			sha256hex:  "invalid-sha256",
			success:    false,
		},
		{
			desc:       "Nested hash reader NewReader() should merge.",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4),
			size:       4,
			actualSize: 4,
			success:    true,
		},
		{
			desc:       "Mismatching sha256",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", 4),
			size:       4,
			actualSize: 4,
			sha256hex:  "50d858e0985ecc7f60418aaf0cc5ab587f42c2570a884095a9e8ccacd0f6545c",
			success:    false,
		},
		{
			desc:       "Correct sha256",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", 4),
			size:       4,
			actualSize: 4,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			success:    true,
		},
		{
			desc:       "Mismatching MD5",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "e2fc714c4727ee9395f324cd2e7f331f", "", 4),
			size:       4,
			actualSize: 4,
			md5hex:     "0773da587b322af3a8718cb418a715ce",
			success:    false,
		},
		{
			desc:       "Correct MD5",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "e2fc714c4727ee9395f324cd2e7f331f", "", 4),
			size:       4,
			actualSize: 4,
			md5hex:     "e2fc714c4727ee9395f324cd2e7f331f",
			success:    true,
		},
		{
			desc:       "Nothing, all ok",
			src:        bytes.NewReader([]byte("abcd")),
			size:       4,
			actualSize: 4,
			success:    true,
		},
		{
			desc:       "Nested, size mismatch",
			src:        mustReader(t, bytes.NewReader([]byte("abcd-morestuff")), 4, "", "", -1),
			size:       2,
			actualSize: -1,
			success:    false,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			_, err := NewReader(t.Context(), testCase.src, testCase.size, testCase.md5hex, testCase.sha256hex, testCase.actualSize)
			if err != nil && testCase.success {
				t.Errorf("Test %q: Expected success, but got error %s instead", testCase.desc, err)
			}
			if err == nil && !testCase.success {
				t.Errorf("Test %q: Expected error, but got success", testCase.desc)
			}
		})
	}
}
