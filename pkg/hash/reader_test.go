/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
 */

package hash

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

// Tests functions like Size(), MD5*(), SHA256*()
func TestHashReaderHelperMethods(t *testing.T) {
	r, err := NewReader(bytes.NewReader([]byte("abcd")), 4, "e2fc714c4727ee9395f324cd2e7f331f", "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", 4, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(ioutil.Discard, r)
	if err != nil {
		t.Fatal(err)
	}
	if r.MD5HexString() != "e2fc714c4727ee9395f324cd2e7f331f" {
		t.Errorf("Expected md5hex \"e2fc714c4727ee9395f324cd2e7f331f\", got %s", r.MD5HexString())
	}
	if r.SHA256HexString() != "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589" {
		t.Errorf("Expected sha256hex \"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589\", got %s", r.SHA256HexString())
	}
	if r.MD5Base64String() != "4vxxTEcn7pOV8yTNLn8zHw==" {
		t.Errorf("Expected md5base64 \"4vxxTEcn7pOV8yTNLn8zHw==\", got \"%s\"", r.MD5Base64String())
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
	if !bytes.Equal(r.MD5(), expectedMD5) {
		t.Errorf("Expected md5hex \"e2fc714c4727ee9395f324cd2e7f331f\", got %s", r.MD5HexString())
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
		{
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
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4, false),
			size:       4,
			actualSize: 4,
		},
		{
			desc:       "Incorrect sha256, nested",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4, false),
			size:       4,
			actualSize: 4,
			sha256hex:  "50d858e0985ecc7f60418aaf0cc5ab587f42c2570a884095a9e8ccacd0f6545c",
			err: SHA256Mismatch{
				ExpectedSHA256:   "50d858e0985ecc7f60418aaf0cc5ab587f42c2570a884095a9e8ccacd0f6545c",
				CalculatedSHA256: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			},
		},
		{
			desc:       "Correct sha256, nested",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4, false),
			size:       4,
			actualSize: 4,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		},
		{
			desc:       "Correct sha256, nested, truncated",
			src:        mustReader(t, bytes.NewReader([]byte("abcd-more-stuff-to-be ignored")), 4, "", "", 4, false),
			size:       4,
			actualSize: -1,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		},
		{
			desc:       "Correct sha256, nested, truncated, swapped",
			src:        mustReader(t, bytes.NewReader([]byte("abcd-more-stuff-to-be ignored")), 4, "", "", -1, false),
			size:       4,
			actualSize: -1,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		},
		{
			desc:       "Incorrect MD5, nested",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4, false),
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
		},
		{
			desc:       "Correct MD5, nested",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4, false),
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
		},
		{
			desc:       "Correct MD5, nested, truncated",
			src:        mustReader(t, bytes.NewReader([]byte("abcd-morestuff")), -1, "", "", -1, false),
			size:       4,
			actualSize: 4,
			md5hex:     "e2fc714c4727ee9395f324cd2e7f331f",
		},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			r, err := NewReader(testCase.src, testCase.size, testCase.md5hex, testCase.sha256hex, testCase.actualSize, false)
			if err != nil {
				t.Fatalf("Test %q: Initializing reader failed %s", testCase.desc, err)
			}
			_, err = io.Copy(ioutil.Discard, r)
			if err != nil {
				if err.Error() != testCase.err.Error() {
					t.Errorf("Test %q: Expected error %s, got error %s", testCase.desc, testCase.err, err)
				}
			}
		})
	}
}

func mustReader(t *testing.T, src io.Reader, size int64, md5Hex, sha256Hex string, actualSize int64, strictCompat bool) *Reader {
	r, err := NewReader(src, size, md5Hex, sha256Hex, actualSize, strictCompat)
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
		expectedErr       error
		strict            bool
	}{
		{
			desc:        "Invalid md5sum NewReader() will fail.",
			src:         bytes.NewReader([]byte("abcd")),
			size:        4,
			actualSize:  4,
			md5hex:      "invalid-md5",
			success:     false,
			expectedErr: BadDigest{},
		},
		{
			desc:        "Invalid sha256 NewReader() will fail.",
			src:         bytes.NewReader([]byte("abcd")),
			size:        4,
			actualSize:  4,
			sha256hex:   "invalid-sha256",
			success:     false,
			expectedErr: SHA256Mismatch{},
		},
		{
			desc:       "Nested hash reader NewReader() should merge.",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "", 4, false),
			size:       4,
			actualSize: 4,
			success:    true,
		},
		{
			desc:        "Mismatching sha256",
			src:         mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", 4, false),
			size:        4,
			actualSize:  4,
			sha256hex:   "50d858e0985ecc7f60418aaf0cc5ab587f42c2570a884095a9e8ccacd0f6545c",
			success:     false,
			expectedErr: SHA256Mismatch{},
		},
		{
			desc:       "Correct sha256",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "", "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", 4, false),
			size:       4,
			actualSize: 4,
			sha256hex:  "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			success:    true,
		},
		{
			desc:        "Mismatching MD5",
			src:         mustReader(t, bytes.NewReader([]byte("abcd")), 4, "e2fc714c4727ee9395f324cd2e7f331f", "", 4, false),
			size:        4,
			actualSize:  4,
			md5hex:      "0773da587b322af3a8718cb418a715ce",
			success:     false,
			expectedErr: BadDigest{},
		},
		{
			desc:       "Correct MD5",
			src:        mustReader(t, bytes.NewReader([]byte("abcd")), 4, "e2fc714c4727ee9395f324cd2e7f331f", "", 4, false),
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
			desc:        "Nested, size mismatch",
			src:         mustReader(t, bytes.NewReader([]byte("abcd-morestuff")), 4, "", "", -1, false),
			size:        2,
			actualSize:  -1,
			success:     false,
			expectedErr: ErrSizeMismatch{Want: 4, Got: 2},
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case-%d", i+1), func(t *testing.T) {
			_, err := NewReader(testCase.src, testCase.size, testCase.md5hex, testCase.sha256hex, testCase.actualSize, testCase.strict)
			if err != nil && testCase.success {
				t.Errorf("Test %q: Expected success, but got error %s instead", testCase.desc, err)
			}
			if err == nil && !testCase.success {
				t.Errorf("Test %q: Expected error, but got success", testCase.desc)
			}
			if !testCase.success {
				if err != testCase.expectedErr {
					t.Errorf("Test %q: Expected error %v, but got %v", testCase.desc, testCase.expectedErr, err)
				}
			}
		})
	}
}
