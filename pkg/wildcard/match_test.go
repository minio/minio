/*
 * MinIO Cloud Storage, (C) 2015, 2016 MinIO, Inc.
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

package wildcard_test

import (
	"testing"

	"github.com/minio/minio/pkg/wildcard"
)

// TestMatch - Tests validate the logic of wild card matching.
// `Match` supports '*' and '?' wildcards.
// Sample usage: In resource matching for bucket policy validation.
func TestMatch(t *testing.T) {
	testCases := []struct {
		pattern string
		text    string
		matched bool
	}{
		// Test case - 1.
		// Test case with pattern "*". Expected to match any text.
		{
			pattern: "*",
			text:    "s3:GetObject",
			matched: true,
		},
		// Test case - 2.
		// Test case with empty pattern. This only matches empty string.
		{
			pattern: "",
			text:    "s3:GetObject",
			matched: false,
		},
		// Test case - 3.
		// Test case with empty pattern. This only matches empty string.
		{
			pattern: "",
			text:    "",
			matched: true,
		},
		// Test case - 4.
		// Test case with single "*" at the end.
		{
			pattern: "s3:*",
			text:    "s3:ListMultipartUploadParts",
			matched: true,
		},
		// Test case - 5.
		// Test case with a no "*". In this case the pattern and text should be the same.
		{
			pattern: "s3:ListBucketMultipartUploads",
			text:    "s3:ListBucket",
			matched: false,
		},
		// Test case - 6.
		// Test case with a no "*". In this case the pattern and text should be the same.
		{
			pattern: "s3:ListBucket",
			text:    "s3:ListBucket",
			matched: true,
		},
		// Test case - 7.
		// Test case with a no "*". In this case the pattern and text should be the same.
		{
			pattern: "s3:ListBucketMultipartUploads",
			text:    "s3:ListBucketMultipartUploads",
			matched: true,
		},
		// Test case - 8.
		// Test case with pattern containing key name with a prefix. Should accept the same text without a "*".
		{
			pattern: "my-bucket/oo*",
			text:    "my-bucket/oo",
			matched: true,
		},
		// Test case - 9.
		// Test case with "*" at the end of the pattern.
		{
			pattern: "my-bucket/In*",
			text:    "my-bucket/India/Karnataka/",
			matched: true,
		},
		// Test case - 10.
		// Test case with prefixes shuffled.
		// This should fail.
		{
			pattern: "my-bucket/In*",
			text:    "my-bucket/Karnataka/India/",
			matched: false,
		},
		// Test case - 11.
		// Test case with text expanded to the wildcards in the pattern.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Ban",
			matched: true,
		},
		// Test case - 12.
		// Test case with the  keyname part is repeated as prefix several times.
		// This is valid.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Ban/Ban/Ban/Ban/Ban",
			matched: true,
		},
		// Test case - 13.
		// Test case to validate that `*` can be expanded into multiple prefixes.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Area1/Area2/Area3/Ban",
			matched: true,
		},
		// Test case - 14.
		// Test case to validate that `*` can be expanded into multiple prefixes.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/State1/State2/Karnataka/Area1/Area2/Area3/Ban",
			matched: true,
		},
		// Test case - 15.
		// Test case where the keyname part of the pattern is expanded in the text.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Bangalore",
			matched: false,
		},
		// Test case - 16.
		// Test case with prefixes and wildcard expanded for all "*".
		{
			pattern: "my-bucket/In*/Ka*/Ban*",
			text:    "my-bucket/India/Karnataka/Bangalore",
			matched: true,
		},
		// Test case - 17.
		// Test case with keyname part being a wildcard in the pattern.
		{
			pattern: "my-bucket/*",
			text:    "my-bucket/India",
			matched: true,
		},
		// Test case - 18.
		{
			pattern: "my-bucket/oo*",
			text:    "my-bucket/odo",
			matched: false,
		},

		// Test case with pattern containing wildcard '?'.
		// Test case - 19.
		// "my-bucket?/" matches "my-bucket1/", "my-bucket2/", "my-bucket3" etc...
		// doesn't match "mybucket/".
		{
			pattern: "my-bucket?/abc*",
			text:    "mybucket/abc",
			matched: false,
		},
		// Test case - 20.
		{
			pattern: "my-bucket?/abc*",
			text:    "my-bucket1/abc",
			matched: true,
		},
		// Test case - 21.
		{
			pattern: "my-?-bucket/abc*",
			text:    "my--bucket/abc",
			matched: false,
		},
		// Test case - 22.
		{
			pattern: "my-?-bucket/abc*",
			text:    "my-1-bucket/abc",
			matched: true,
		},
		// Test case - 23.
		{
			pattern: "my-?-bucket/abc*",
			text:    "my-k-bucket/abc",
			matched: true,
		},
		// Test case - 24.
		{
			pattern: "my??bucket/abc*",
			text:    "mybucket/abc",
			matched: false,
		},
		// Test case - 25.
		{
			pattern: "my??bucket/abc*",
			text:    "my4abucket/abc",
			matched: true,
		},
		// Test case - 26.
		{
			pattern: "my-bucket?abc*",
			text:    "my-bucket/abc",
			matched: true,
		},
		// Test case 27-28.
		// '?' matches '/' too. (works with s3).
		// This is because the namespace is considered flat.
		// "abc?efg" matches both "abcdefg" and "abc/efg".
		{
			pattern: "my-bucket/abc?efg",
			text:    "my-bucket/abcdefg",
			matched: true,
		},
		{
			pattern: "my-bucket/abc?efg",
			text:    "my-bucket/abc/efg",
			matched: true,
		},
		// Test case - 29.
		{
			pattern: "my-bucket/abc????",
			text:    "my-bucket/abc",
			matched: false,
		},
		// Test case - 30.
		{
			pattern: "my-bucket/abc????",
			text:    "my-bucket/abcde",
			matched: false,
		},
		// Test case - 31.
		{
			pattern: "my-bucket/abc????",
			text:    "my-bucket/abcdefg",
			matched: true,
		},
		// Test case 32-34.
		// test case with no '*'.
		{
			pattern: "my-bucket/abc?",
			text:    "my-bucket/abc",
			matched: false,
		},
		{
			pattern: "my-bucket/abc?",
			text:    "my-bucket/abcd",
			matched: true,
		},
		{
			pattern: "my-bucket/abc?",
			text:    "my-bucket/abcde",
			matched: false,
		},
		// Test case 35.
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnop",
			matched: false,
		},
		// Test case 36.
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopqrst/mnopqr",
			matched: true,
		},
		// Test case 37.
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopqrst/mnopqrs",
			matched: true,
		},
		// Test case 38.
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnop",
			matched: false,
		},
		// Test case 39.
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopq",
			matched: true,
		},
		// Test case 40.
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopqr",
			matched: true,
		},
		// Test case 41.
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopqand",
			matched: true,
		},
		// Test case 42.
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopand",
			matched: false,
		},
		// Test case 43.
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopqand",
			matched: true,
		},
		// Test case 44.
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mn",
			matched: false,
		},
		// Test case 45.
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopqrst/mnopqrs",
			matched: true,
		},
		// Test case 46.
		{
			pattern: "my-bucket/mnop*??",
			text:    "my-bucket/mnopqrst",
			matched: true,
		},
		// Test case 47.
		{
			pattern: "my-bucket/mnop*qrst",
			text:    "my-bucket/mnopabcdegqrst",
			matched: true,
		},
		// Test case 48.
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopqand",
			matched: true,
		},
		// Test case 49.
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopand",
			matched: false,
		},
		// Test case 50.
		{
			pattern: "my-bucket/mnop*?and?",
			text:    "my-bucket/mnopqanda",
			matched: true,
		},
		// Test case 51.
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopqanda",
			matched: false,
		},
		// Test case 52.

		{
			pattern: "my-?-bucket/abc*",
			text:    "my-bucket/mnopqanda",
			matched: false,
		},
	}
	// Iterating over the test cases, call the function under test and asert the output.
	for i, testCase := range testCases {
		actualResult := wildcard.Match(testCase.pattern, testCase.text)
		if testCase.matched != actualResult {
			t.Errorf("Test %d: Expected the result to be `%v`, but instead found it to be `%v`", i+1, testCase.matched, actualResult)
		}
	}
}

// TestMatchSimple - Tests validate the logic of wild card matching.
// `MatchSimple` supports matching for only '*' in the pattern string.
func TestMatchSimple(t *testing.T) {
	testCases := []struct {
		pattern string
		text    string
		matched bool
	}{
		// Test case - 1.
		// Test case with pattern "*". Expected to match any text.
		{
			pattern: "*",
			text:    "s3:GetObject",
			matched: true,
		},
		// Test case - 2.
		// Test case with empty pattern. This only matches empty string.
		{
			pattern: "",
			text:    "s3:GetObject",
			matched: false,
		},
		// Test case - 3.
		// Test case with empty pattern. This only matches empty string.
		{
			pattern: "",
			text:    "",
			matched: true,
		},
		// Test case - 4.
		// Test case with single "*" at the end.
		{
			pattern: "s3:*",
			text:    "s3:ListMultipartUploadParts",
			matched: true,
		},
		// Test case - 5.
		// Test case with a no "*". In this case the pattern and text should be the same.
		{
			pattern: "s3:ListBucketMultipartUploads",
			text:    "s3:ListBucket",
			matched: false,
		},
		// Test case - 6.
		// Test case with a no "*". In this case the pattern and text should be the same.
		{
			pattern: "s3:ListBucket",
			text:    "s3:ListBucket",
			matched: true,
		},
		// Test case - 7.
		// Test case with a no "*". In this case the pattern and text should be the same.
		{
			pattern: "s3:ListBucketMultipartUploads",
			text:    "s3:ListBucketMultipartUploads",
			matched: true,
		},
		// Test case - 8.
		// Test case with pattern containing key name with a prefix. Should accept the same text without a "*".
		{
			pattern: "my-bucket/oo*",
			text:    "my-bucket/oo",
			matched: true,
		},
		// Test case - 9.
		// Test case with "*" at the end of the pattern.
		{
			pattern: "my-bucket/In*",
			text:    "my-bucket/India/Karnataka/",
			matched: true,
		},
		// Test case - 10.
		// Test case with prefixes shuffled.
		// This should fail.
		{
			pattern: "my-bucket/In*",
			text:    "my-bucket/Karnataka/India/",
			matched: false,
		},
		// Test case - 11.
		// Test case with text expanded to the wildcards in the pattern.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Ban",
			matched: true,
		},
		// Test case - 12.
		// Test case with the  keyname part is repeated as prefix several times.
		// This is valid.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Ban/Ban/Ban/Ban/Ban",
			matched: true,
		},
		// Test case - 13.
		// Test case to validate that `*` can be expanded into multiple prefixes.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Area1/Area2/Area3/Ban",
			matched: true,
		},
		// Test case - 14.
		// Test case to validate that `*` can be expanded into multiple prefixes.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/State1/State2/Karnataka/Area1/Area2/Area3/Ban",
			matched: true,
		},
		// Test case - 15.
		// Test case where the keyname part of the pattern is expanded in the text.
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Bangalore",
			matched: false,
		},
		// Test case - 16.
		// Test case with prefixes and wildcard expanded for all "*".
		{
			pattern: "my-bucket/In*/Ka*/Ban*",
			text:    "my-bucket/India/Karnataka/Bangalore",
			matched: true,
		},
		// Test case - 17.
		// Test case with keyname part being a wildcard in the pattern.
		{
			pattern: "my-bucket/*",
			text:    "my-bucket/India",
			matched: true,
		},
		// Test case - 18.
		{
			pattern: "my-bucket/oo*",
			text:    "my-bucket/odo",
			matched: false,
		},
		// Test case - 11.
		{
			pattern: "my-bucket/oo?*",
			text:    "my-bucket/oo???",
			matched: true,
		},
		// Test case - 12:
		{
			pattern: "my-bucket/oo??*",
			text:    "my-bucket/odo",
			matched: false,
		},
		// Test case - 13:
		{
			pattern: "?h?*",
			text:    "?h?hello",
			matched: true,
		},
	}
	// Iterating over the test cases, call the function under test and asert the output.
	for i, testCase := range testCases {
		actualResult := wildcard.MatchSimple(testCase.pattern, testCase.text)
		if testCase.matched != actualResult {
			t.Errorf("Test %d: Expected the result to be `%v`, but instead found it to be `%v`", i+1, testCase.matched, actualResult)
		}
	}
}
