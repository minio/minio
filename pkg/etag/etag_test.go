// MinIO Cloud Storage, (C) 2021 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etag

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

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
		reader := NewReader(strings.NewReader(test.Content), test.ETag)
		if _, err := io.Copy(ioutil.Discard, reader); err != nil {
			t.Fatalf("Test %d: read failed: %v", i, err)
		}
		if ETag := reader.ETag(); !Equal(ETag, test.ETag) {
			t.Fatalf("Test %d: ETag mismatch: got %q - want %q", i, ETag, test.ETag)
		}
	}
}
