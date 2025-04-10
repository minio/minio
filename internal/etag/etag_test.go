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

package etag

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

var _ Tagger = Wrap(nil, nil).(Tagger) // runtime check that wrapReader implements Tagger

var parseTests = []struct {
	String     string
	ETag       ETag
	ShouldFail bool
}{
	{String: "3b83ef96387f1465", ETag: ETag{59, 131, 239, 150, 56, 127, 20, 101}},                                                                    // 0
	{String: "3b83ef96387f14655fc854ddc3c6bd57", ETag: ETag{59, 131, 239, 150, 56, 127, 20, 101, 95, 200, 84, 221, 195, 198, 189, 87}},               // 1
	{String: `"3b83ef96387f14655fc854ddc3c6bd57"`, ETag: ETag{59, 131, 239, 150, 56, 127, 20, 101, 95, 200, 84, 221, 195, 198, 189, 87}},             // 2
	{String: "ceb8853ddc5086cc4ab9e149f8f09c88-1", ETag: ETag{206, 184, 133, 61, 220, 80, 134, 204, 74, 185, 225, 73, 248, 240, 156, 136, 45, 49}},   // 3
	{String: `"ceb8853ddc5086cc4ab9e149f8f09c88-2"`, ETag: ETag{206, 184, 133, 61, 220, 80, 134, 204, 74, 185, 225, 73, 248, 240, 156, 136, 45, 50}}, // 4
	{ // 5
		String: "90402c78d2dccddee1e9e86222ce2c6361675f3529d26000ae2e900ff216b3cb59e130e092d8a2981e776f4d0bd60941",
		ETag:   ETag{144, 64, 44, 120, 210, 220, 205, 222, 225, 233, 232, 98, 34, 206, 44, 99, 97, 103, 95, 53, 41, 210, 96, 0, 174, 46, 144, 15, 242, 22, 179, 203, 89, 225, 48, 224, 146, 216, 162, 152, 30, 119, 111, 77, 11, 214, 9, 65},
	},

	{String: `"3b83ef96387f14655fc854ddc3c6bd57`, ShouldFail: true},                                                                  // 6
	{String: "ceb8853ddc5086cc4ab9e149f8f09c88-", ShouldFail: true},                                                                  // 7
	{String: "ceb8853ddc5086cc4ab9e149f8f09c88-2a", ShouldFail: true},                                                                // 8
	{String: "ceb8853ddc5086cc4ab9e149f8f09c88-2-1", ShouldFail: true},                                                               // 9
	{String: "90402c78d2dccddee1e9e86222ce2c-1", ShouldFail: true},                                                                   // 10
	{String: "90402c78d2dccddee1e9e86222ce2c6361675f3529d26000ae2e900ff216b3cb59e130e092d8a2981e776f4d0bd60941-1", ShouldFail: true}, // 11
}

func TestParse(t *testing.T) {
	for i, test := range parseTests {
		etag, err := Parse(test.String)
		if err == nil && test.ShouldFail {
			t.Fatalf("Test %d: parse should have failed but succeeded", i)
		}
		if err != nil && !test.ShouldFail {
			t.Fatalf("Test %d: failed to parse ETag %q: %v", i, test.String, err)
		}
		if !Equal(etag, test.ETag) {
			t.Log([]byte(etag))
			t.Fatalf("Test %d: ETags don't match", i)
		}
	}
}

var stringTests = []struct {
	ETag   ETag
	String string
}{
	{ETag: ETag{59, 131, 239, 150, 56, 127, 20, 101}, String: "3b83ef96387f1465"},                                                                  // 0
	{ETag: ETag{59, 131, 239, 150, 56, 127, 20, 101, 95, 200, 84, 221, 195, 198, 189, 87}, String: "3b83ef96387f14655fc854ddc3c6bd57"},             // 1
	{ETag: ETag{206, 184, 133, 61, 220, 80, 134, 204, 74, 185, 225, 73, 248, 240, 156, 136, 45, 49}, String: "ceb8853ddc5086cc4ab9e149f8f09c88-1"}, // 2
	{ETag: ETag{206, 184, 133, 61, 220, 80, 134, 204, 74, 185, 225, 73, 248, 240, 156, 136, 45, 50}, String: "ceb8853ddc5086cc4ab9e149f8f09c88-2"}, // 3
	{ // 4
		ETag:   ETag{144, 64, 44, 120, 210, 220, 205, 222, 225, 233, 232, 98, 34, 206, 44, 99, 97, 103, 95, 53, 41, 210, 96, 0, 174, 46, 144, 15, 242, 22, 179, 203, 89, 225, 48, 224, 146, 216, 162, 152, 30, 119, 111, 77, 11, 214, 9, 65},
		String: "90402c78d2dccddee1e9e86222ce2c6361675f3529d26000ae2e900ff216b3cb59e130e092d8a2981e776f4d0bd60941",
	},
	{ // 5
		ETag:   ETag{32, 0, 15, 0, 219, 45, 144, 167, 180, 7, 130, 212, 207, 242, 180, 26, 119, 153, 252, 30, 126, 173, 37, 151, 45, 182, 81, 80, 17, 141, 251, 226, 186, 118, 163, 192, 2, 218, 40, 248, 92, 132, 12, 210, 0, 26, 40, 169},
		String: "20000f00db2d90a7b40782d4cff2b41a7799fc1e7ead25972db65150118dfbe2ba76a3c002da28f85c840cd2001a28a9",
	},
}

func TestString(t *testing.T) {
	for i, test := range stringTests {
		s := test.ETag.String()
		if s != test.String {
			t.Fatalf("Test %d: got %s - want %s", i, s, test.String)
		}
	}
}

var equalTests = []struct {
	A     string
	B     string
	Equal bool
}{
	{A: "3b83ef96387f14655fc854ddc3c6bd57", B: "3b83ef96387f14655fc854ddc3c6bd57", Equal: true},   // 0
	{A: "3b83ef96387f14655fc854ddc3c6bd57", B: `"3b83ef96387f14655fc854ddc3c6bd57"`, Equal: true}, // 1

	{A: "3b83ef96387f14655fc854ddc3c6bd57", B: "3b83ef96387f14655fc854ddc3c6bd57-2", Equal: false}, // 2
	{A: "3b83ef96387f14655fc854ddc3c6bd57", B: "ceb8853ddc5086cc4ab9e149f8f09c88", Equal: false},   // 3
}

func TestEqual(t *testing.T) {
	for i, test := range equalTests {
		A, err := Parse(test.A)
		if err != nil {
			t.Fatalf("Test %d: %v", i, err)
		}
		B, err := Parse(test.B)
		if err != nil {
			t.Fatalf("Test %d: %v", i, err)
		}
		if equal := Equal(A, B); equal != test.Equal {
			t.Fatalf("Test %d: got %v - want %v", i, equal, test.Equal)
		}
	}
}

var readerTests = []struct { // Reference values computed by: echo <content> | md5sum
	Content string
	ETag    ETag
}{
	{
		Content: "", ETag: ETag{212, 29, 140, 217, 143, 0, 178, 4, 233, 128, 9, 152, 236, 248, 66, 126},
	},
	{
		Content: " ", ETag: ETag{114, 21, 238, 156, 125, 157, 194, 41, 210, 146, 26, 64, 232, 153, 236, 95},
	},
	{
		Content: "Hello World", ETag: ETag{177, 10, 141, 177, 100, 224, 117, 65, 5, 183, 169, 155, 231, 46, 63, 229},
	},
}

func TestReader(t *testing.T) {
	for i, test := range readerTests {
		reader := NewReader(t.Context(), strings.NewReader(test.Content), test.ETag, nil)
		if _, err := io.Copy(io.Discard, reader); err != nil {
			t.Fatalf("Test %d: read failed: %v", i, err)
		}
		if ETag := reader.ETag(); !Equal(ETag, test.ETag) {
			t.Fatalf("Test %d: ETag mismatch: got %q - want %q", i, ETag, test.ETag)
		}
	}
}

var multipartTests = []struct { // Test cases have been generated using AWS S3
	ETags     []ETag
	Multipart ETag
}{
	{
		ETags:     []ETag{},
		Multipart: ETag{},
	},
	{
		ETags:     []ETag{must("b10a8db164e0754105b7a99be72e3fe5")},
		Multipart: must("7b976cc68452e003eec7cb0eb631a19a-1"),
	},
	{
		ETags:     []ETag{must("5f363e0e58a95f06cbe9bbc662c5dfb6"), must("5f363e0e58a95f06cbe9bbc662c5dfb6")},
		Multipart: must("a7d414b9133d6483d9a1c4e04e856e3b-2"),
	},
	{
		ETags:     []ETag{must("5f363e0e58a95f06cbe9bbc662c5dfb6"), must("a096eb5968d607c2975fb2c4af9ab225"), must("b10a8db164e0754105b7a99be72e3fe5")},
		Multipart: must("9a0d1febd9265f59f368ceb652770bc2-3"),
	},
	{ // Check that multipart ETags are ignored
		ETags:     []ETag{must("5f363e0e58a95f06cbe9bbc662c5dfb6"), must("5f363e0e58a95f06cbe9bbc662c5dfb6"), must("ceb8853ddc5086cc4ab9e149f8f09c88-1")},
		Multipart: must("a7d414b9133d6483d9a1c4e04e856e3b-2"),
	},
	{ // Check that encrypted ETags are ignored
		ETags: []ETag{
			must("90402c78d2dccddee1e9e86222ce2c6361675f3529d26000ae2e900ff216b3cb59e130e092d8a2981e776f4d0bd60941"),
			must("5f363e0e58a95f06cbe9bbc662c5dfb6"), must("5f363e0e58a95f06cbe9bbc662c5dfb6"),
		},
		Multipart: must("a7d414b9133d6483d9a1c4e04e856e3b-2"),
	},
}

func TestMultipart(t *testing.T) {
	for i, test := range multipartTests {
		if multipart := Multipart(test.ETags...); !Equal(multipart, test.Multipart) {
			t.Fatalf("Test %d: got %q - want %q", i, multipart, test.Multipart)
		}
	}
}

var isEncryptedTests = []struct {
	ETag        string
	IsEncrypted bool
}{
	{ETag: "20000f00db2d90a7b40782d4cff2b41a7799fc1e7ead25972db65150118dfbe2ba76a3c002da28f85c840cd2001a28a9", IsEncrypted: true}, // 0

	{ETag: "3b83ef96387f14655fc854ddc3c6bd57"},       // 1
	{ETag: "7b976cc68452e003eec7cb0eb631a19a-1"},     // 2
	{ETag: "a7d414b9133d6483d9a1c4e04e856e3b-2"},     // 3
	{ETag: "7b976cc68452e003eec7cb0eb631a19a-10000"}, // 4
}

func TestIsEncrypted(t *testing.T) {
	for i, test := range isEncryptedTests {
		tag, err := Parse(test.ETag)
		if err != nil {
			t.Fatalf("Test %d: failed to parse ETag: %v", i, err)
		}
		if isEncrypted := tag.IsEncrypted(); isEncrypted != test.IsEncrypted {
			t.Fatalf("Test %d: got '%v' - want '%v'", i, isEncrypted, test.IsEncrypted)
		}
	}
}

var formatTests = []struct {
	ETag    string
	AWSETag string
}{
	{ETag: "3b83ef96387f14655fc854ddc3c6bd57", AWSETag: "3b83ef96387f14655fc854ddc3c6bd57"},                                                                 // 0
	{ETag: "7b976cc68452e003eec7cb0eb631a19a-1", AWSETag: "7b976cc68452e003eec7cb0eb631a19a-1"},                                                             // 1
	{ETag: "a7d414b9133d6483d9a1c4e04e856e3b-2", AWSETag: "a7d414b9133d6483d9a1c4e04e856e3b-2"},                                                             // 2
	{ETag: "7b976cc68452e003eec7cb0eb631a19a-10000", AWSETag: "7b976cc68452e003eec7cb0eb631a19a-10000"},                                                     // 3
	{ETag: "20000f00db2d90a7b40782d4cff2b41a7799fc1e7ead25972db65150118dfbe2ba76a3c002da28f85c840cd2001a28a9", AWSETag: "ba76a3c002da28f85c840cd2001a28a9"}, // 4
}

func TestFormat(t *testing.T) {
	for i, test := range formatTests {
		tag, err := Parse(test.ETag)
		if err != nil {
			t.Fatalf("Test %d: failed to parse ETag: %v", i, err)
		}
		if s := tag.Format().String(); s != test.AWSETag {
			t.Fatalf("Test %d: got '%v' - want '%v'", i, s, test.AWSETag)
		}
	}
}

var fromContentMD5Tests = []struct {
	Header     http.Header
	ETag       ETag
	ShouldFail bool
}{
	{Header: http.Header{}, ETag: nil}, // 0
	{Header: http.Header{"Content-Md5": []string{"1B2M2Y8AsgTpgAmY7PhCfg=="}}, ETag: must("d41d8cd98f00b204e9800998ecf8427e")},                             // 1
	{Header: http.Header{"Content-Md5": []string{"sQqNsWTgdUEFt6mb5y4/5Q=="}}, ETag: must("b10a8db164e0754105b7a99be72e3fe5")},                             // 2
	{Header: http.Header{"Content-MD5": []string{"1B2M2Y8AsgTpgAmY7PhCfg=="}}, ETag: nil},                                                                  // 3 (Content-MD5 vs Content-Md5)
	{Header: http.Header{"Content-Md5": []string{"sQqNsWTgdUEFt6mb5y4/5Q==", "1B2M2Y8AsgTpgAmY7PhCfg=="}}, ETag: must("b10a8db164e0754105b7a99be72e3fe5")}, // 4

	{Header: http.Header{"Content-Md5": []string{""}}, ShouldFail: true},                                 // 5 (empty value)
	{Header: http.Header{"Content-Md5": []string{"", "sQqNsWTgdUEFt6mb5y4/5Q=="}}, ShouldFail: true},     // 6 (empty value)
	{Header: http.Header{"Content-Md5": []string{"d41d8cd98f00b204e9800998ecf8427e"}}, ShouldFail: true}, // 7 (content-md5 is invalid b64 / of invalid length)
}

func TestFromContentMD5(t *testing.T) {
	for i, test := range fromContentMD5Tests {
		ETag, err := FromContentMD5(test.Header)
		if err != nil && !test.ShouldFail {
			t.Fatalf("Test %d: failed to convert Content-MD5 to ETag: %v", i, err)
		}
		if err == nil && test.ShouldFail {
			t.Fatalf("Test %d: should have failed but succeeded", i)
		}
		if err == nil {
			if !Equal(ETag, test.ETag) {
				t.Fatalf("Test %d: got %q - want %q", i, ETag, test.ETag)
			}
		}
	}
}

var decryptTests = []struct {
	Key       []byte
	ETag      ETag
	Plaintext ETag
}{
	{ // 0
		Key:       make([]byte, 32),
		ETag:      must("3b83ef96387f14655fc854ddc3c6bd57"),
		Plaintext: must("3b83ef96387f14655fc854ddc3c6bd57"),
	},
	{ // 1
		Key:       make([]byte, 32),
		ETag:      must("7b976cc68452e003eec7cb0eb631a19a-1"),
		Plaintext: must("7b976cc68452e003eec7cb0eb631a19a-1"),
	},
	{ // 2
		Key:       make([]byte, 32),
		ETag:      must("7b976cc68452e003eec7cb0eb631a19a-10000"),
		Plaintext: must("7b976cc68452e003eec7cb0eb631a19a-10000"),
	},
	{ // 3
		Key:       make([]byte, 32),
		ETag:      must("20000f00f2cc184414bc982927ec56abb7e18426faa205558982e9a8125c1370a9cf5754406e428b3343f21ee1125965"),
		Plaintext: must("6d6cdccb9a7498c871bde8eab2f49141"),
	},
}

func TestDecrypt(t *testing.T) {
	for i, test := range decryptTests {
		etag, err := Decrypt(test.Key, test.ETag)
		if err != nil {
			t.Fatalf("Test %d: failed to decrypt ETag: %v", i, err)
		}
		if !Equal(etag, test.Plaintext) {
			t.Fatalf("Test %d: got '%v' - want '%v'", i, etag, test.Plaintext)
		}
	}
}

func must(s string) ETag {
	t, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return t
}
