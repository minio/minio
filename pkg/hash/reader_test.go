/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"io"
	"io/ioutil"
	"testing"
)

// Tests functions like Size(), MD5*(), SHA256*()
func TestHashReaderHelperMethods(t *testing.T) {
	r, err := NewReader(bytes.NewReader([]byte("abcd")), 4, "e2fc714c4727ee9395f324cd2e7f331f", "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(ioutil.Discard, r)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(r.MD5) != "e2fc714c4727ee9395f324cd2e7f331f" {
		t.Errorf("Expected md5hex \"e2fc714c4727ee9395f324cd2e7f331f\", got %s", hex.EncodeToString(r.MD5))
	}
	if hex.EncodeToString(r.SHA256) != "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589" {
		t.Errorf("Expected sha256hex \"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589\", got %s", hex.EncodeToString(r.SHA256))
	}
	if r.Size() != 4 {
		t.Errorf("Expected size 4, got %d", r.Size())
	}
	expectedMD5, err := hex.DecodeString("e2fc714c4727ee9395f324cd2e7f331f")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.MD5, expectedMD5) {
		t.Errorf("Expected md5hex \"e2fc714c4727ee9395f324cd2e7f331f\", got %s", hex.EncodeToString(r.MD5))
	}
	expectedSHA256, err := hex.DecodeString("88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589")
	if !bytes.Equal(r.SHA256, expectedSHA256) {
		t.Errorf("Expected md5hex \"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589\", got %s", hex.EncodeToString(r.SHA256))
	}
}

// Tests hash reader checksum verification.
func TestHashReaderVerification(t *testing.T) {
	testCases := []struct {
		src               io.Reader
		size              int64
		md5hex, sha256hex string
		err               error
	}{
		// Success, no checksum verification provided.
		{
			src:  bytes.NewReader([]byte("abcd")),
			size: 4,
		},
		// Failure md5 mismatch.
		{
			src:    bytes.NewReader([]byte("abcd")),
			size:   4,
			md5hex: "d41d8cd98f00b204e9800998ecf8427f",
			err: BadDigest{
				"d41d8cd98f00b204e9800998ecf8427f",
				"e2fc714c4727ee9395f324cd2e7f331f",
			},
		},
		// Failure sha256 mismatch.
		{
			src:       bytes.NewReader([]byte("abcd")),
			size:      4,
			sha256hex: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580",
			err: SHA256Mismatch{
				"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580",
				"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
			},
		},
	}
	for i, testCase := range testCases {
		r, err := NewReader(testCase.src, testCase.size, testCase.md5hex, testCase.sha256hex)
		if err != nil {
			t.Fatalf("Test %d: Initializing reader failed %s", i+1, err)
		}
		_, err = io.Copy(ioutil.Discard, r)
		if err != nil {
			if err.Error() != testCase.err.Error() {
				t.Errorf("Test %d: Expected error %s, got error %s", i+1, testCase.err, err)
			}
		}
	}
}

// Tests NewReader() constructor with invalid arguments.
func TestHashReaderInvalidArguments(t *testing.T) {
	testCases := []struct {
		src               io.Reader
		size              int64
		md5hex, sha256hex string
		success           bool
	}{
		// Invalid md5sum NewReader() will fail.
		{
			src:     bytes.NewReader([]byte("abcd")),
			size:    4,
			md5hex:  "invalid-md5",
			success: false,
		},
		// Invalid sha256 NewReader() will fail.
		{
			src:       bytes.NewReader([]byte("abcd")),
			size:      4,
			sha256hex: "invalid-sha256",
			success:   false,
		},
		// Nested hash reader NewReader() will fail.
		{
			src: func() io.Reader {
				r, _ := NewReader(bytes.NewReader([]byte("abcd")), 0, "", "")
				return r
			}(),
			size:    4,
			success: false,
		},
		// Expected inputs, NewReader() will succeed.
		{
			src:     bytes.NewReader([]byte("abcd")),
			size:    4,
			success: true,
		},
	}

	for i, testCase := range testCases {
		_, err := NewReader(testCase.src, testCase.size, testCase.md5hex, testCase.sha256hex)
		if err != nil && testCase.success {
			t.Errorf("Test %d: Expected success, but got error %s instead", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Errorf("Test %d: Expected error, but got success", i+1)
		}
	}
}

var gatewayReaderTests = []struct {
	md5Sum, sha256Sum string
	size              int64
}{
	{md5Sum: "", sha256Sum: "", size: 10},
	{md5Sum: "", sha256Sum: "", size: 12},
	{md5Sum: "e2fc714c4727ee9395f324cd2e7f331f", sha256Sum: "", size: 1024},
	{md5Sum: "", sha256Sum: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", size: 20},
	{md5Sum: "e2fc714c4727ee9395f324cd2e7f331f", sha256Sum: "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589", size: 17},
	{md5Sum: "", sha256Sum: "", size: -1},
	{md5Sum: "", sha256Sum: "", size: -2},
}

func TestNewGatewayReader(t *testing.T) {
	for i, test := range gatewayReaderTests {
		reader, err := NewGatewayReader(bytes.NewReader(make([]byte, 1024)), test.size, test.md5Sum, test.sha256Sum)
		if err != nil {
			t.Fatalf("Test %d: failed to create reader %v", i, err)
		}
		if test.size >= 0 && reader.Size() != test.size {
			t.Errorf("Test %d: invalid size - got: %d want: %d", i, reader.Size(), test.size)
		}
		if test.size < 0 && reader.Size() != -1 {
			t.Errorf("Test %d: invalid size - got: %d want: %d", i, reader.Size(), -1)
		}
		if etag := hex.EncodeToString(reader.Etag()); etag != test.md5Sum {
			t.Errorf("Test %d: invalid etag - got: %s want: %s", i, etag, test.md5Sum)
		}
	}
}

func TestNewGatewayReading(t *testing.T) {
	for i, test := range gatewayReaderTests {
		src := bytes.NewReader(make([]byte, 100+int64(i)+test.size))
		reader, err := NewGatewayReader(src, test.size, test.md5Sum, test.sha256Sum)
		if err != nil {
			t.Fatalf("Test %d: failed to create reader %v", i, err)
		}
		if n, _ := io.Copy(ioutil.Discard, reader); test.size >= 0 && n != test.size {
			t.Errorf("Test %d: gateway reader reads wrong number of bytes got: %d want: %d", i, n, test.size)
		}
	}
}
