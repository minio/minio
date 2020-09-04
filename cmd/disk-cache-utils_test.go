/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package cmd

import (
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestGetCacheControlOpts(t *testing.T) {
	expiry, _ := time.Parse(http.TimeFormat, "Wed, 21 Oct 2015 07:28:00 GMT")

	testCases := []struct {
		cacheControlHeaderVal string
		expiryHeaderVal       time.Time
		expectedCacheControl  *cacheControl
		expectedErr           bool
	}{
		{"", timeSentinel, nil, false},
		{"max-age=2592000, public", timeSentinel, &cacheControl{maxAge: 2592000, sMaxAge: 0, minFresh: 0, expiry: time.Time{}}, false},
		{"max-age=2592000, no-store", timeSentinel, &cacheControl{maxAge: 2592000, sMaxAge: 0, noStore: true, minFresh: 0, expiry: time.Time{}}, false},
		{"must-revalidate, max-age=600", timeSentinel, &cacheControl{maxAge: 600, sMaxAge: 0, minFresh: 0, expiry: time.Time{}}, false},
		{"s-maxAge=2500, max-age=600", timeSentinel, &cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Time{}}, false},
		{"s-maxAge=2500, max-age=600", expiry, &cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Date(2015, time.October, 21, 07, 28, 00, 00, time.UTC)}, false},
		{"s-maxAge=2500, max-age=600s", timeSentinel, &cacheControl{maxAge: 600, sMaxAge: 2500, minFresh: 0, expiry: time.Time{}}, true},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			m := make(map[string]string)
			m["cache-control"] = testCase.cacheControlHeaderVal
			if !testCase.expiryHeaderVal.Equal(timeSentinel) {
				m["expires"] = testCase.expiryHeaderVal.String()
			}
			c := cacheControlOpts(ObjectInfo{UserDefined: m, Expires: testCase.expiryHeaderVal})
			if testCase.expectedErr && (c != nil) {
				t.Errorf("expected err, got <nil>")
			}
			if !testCase.expectedErr && !reflect.DeepEqual(c, testCase.expectedCacheControl) {
				t.Errorf("expected %v, got %v", testCase.expectedCacheControl, c)
			}
		})
	}
}

func TestIsMetadataSame(t *testing.T) {

	testCases := []struct {
		m1       map[string]string
		m2       map[string]string
		expected bool
	}{
		{nil, nil, true},
		{nil, map[string]string{}, false},
		{map[string]string{"k": "v"}, map[string]string{"k": "v"}, true},
		{map[string]string{"k": "v"}, map[string]string{"a": "b"}, false},
		{map[string]string{"k1": "v1", "k2": "v2"}, map[string]string{"k1": "v1", "k2": "v1"}, false},
		{map[string]string{"k1": "v1", "k2": "v2"}, map[string]string{"k1": "v1", "k2": "v2"}, true},
		{map[string]string{"K1": "v1", "k2": "v2"}, map[string]string{"k1": "v1", "k2": "v2"}, false},
		{map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"}, map[string]string{"k1": "v1", "k2": "v2"}, false},
	}

	for i, testCase := range testCases {
		actual := isMetadataSame(testCase.m1, testCase.m2)
		if testCase.expected != actual {
			t.Errorf("test %d expected %v, got %v", i, testCase.expected, actual)
		}
	}
}

func TestNewFileScorer(t *testing.T) {
	fs, err := newFileScorer(1000, time.Now().Unix(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs.fileNames()) != 0 {
		t.Fatal("non zero files??")
	}
	now := time.Now()
	fs.addFile("recent", now.Add(-time.Minute), 1000, 10)
	fs.addFile("older", now.Add(-time.Hour), 1000, 10)
	if !reflect.DeepEqual(fs.fileNames(), []string{"older"}) {
		t.Fatal("unexpected file list", fs.queueString())
	}
	fs.reset()
	fs.addFile("bigger", now.Add(-time.Minute), 2000, 10)
	fs.addFile("recent", now.Add(-time.Minute), 1000, 10)
	if !reflect.DeepEqual(fs.fileNames(), []string{"bigger"}) {
		t.Fatal("unexpected file list", fs.queueString())
	}
	fs.reset()
	fs.addFile("less", now.Add(-time.Minute), 1000, 5)
	fs.addFile("recent", now.Add(-time.Minute), 1000, 10)
	if !reflect.DeepEqual(fs.fileNames(), []string{"less"}) {
		t.Fatal("unexpected file list", fs.queueString())
	}
	fs.reset()
	fs.addFile("small", now.Add(-time.Minute), 200, 10)
	fs.addFile("medium", now.Add(-time.Minute), 300, 10)
	if !reflect.DeepEqual(fs.fileNames(), []string{"medium", "small"}) {
		t.Fatal("unexpected file list", fs.queueString())
	}
	fs.addFile("large", now.Add(-time.Minute), 700, 10)
	fs.addFile("xsmol", now.Add(-time.Minute), 7, 10)
	if !reflect.DeepEqual(fs.fileNames(), []string{"large", "medium"}) {
		t.Fatal("unexpected file list", fs.queueString())
	}

	fs.reset()
	fs.addFile("less", now.Add(-time.Minute), 500, 5)
	fs.addFile("recent", now.Add(-time.Minute), 500, 10)
	if !fs.adjustSaveBytes(-500) {
		t.Fatal("we should still need more bytes, got false")
	}
	// We should only need 500 bytes now.
	if !reflect.DeepEqual(fs.fileNames(), []string{"less"}) {
		t.Fatal("unexpected file list", fs.queueString())
	}
	if fs.adjustSaveBytes(-500) {
		t.Fatal("we shouldn't need any more bytes, got true")
	}
	fs, err = newFileScorer(1000, time.Now().Unix(), 10)
	if err != nil {
		t.Fatal(err)
	}
	fs.addFile("bigger", now.Add(-time.Minute), 50, 10)
	// sorting should be consistent after adjusting savebytes.
	fs.adjustSaveBytes(-800)
	fs.addFile("smaller", now.Add(-time.Minute), 40, 10)
	if !reflect.DeepEqual(fs.fileNames(), []string{"bigger", "smaller"}) {
		t.Fatal("unexpected file list", fs.queueString())
	}
}
func TestBytesToClear(t *testing.T) {
	testCases := []struct {
		total         int64
		free          int64
		quotaPct      uint64
		watermarkLow  uint64
		watermarkHigh uint64
		expected      uint64
	}{
		{total: 1000, free: 800, quotaPct: 40, watermarkLow: 90, watermarkHigh: 90, expected: 0},
		{total: 1000, free: 200, quotaPct: 40, watermarkLow: 90, watermarkHigh: 90, expected: 400},
		{total: 1000, free: 400, quotaPct: 40, watermarkLow: 90, watermarkHigh: 90, expected: 240},
		{total: 1000, free: 600, quotaPct: 40, watermarkLow: 90, watermarkHigh: 90, expected: 40},
		{total: 1000, free: 600, quotaPct: 40, watermarkLow: 70, watermarkHigh: 70, expected: 120},
		{total: 1000, free: 1000, quotaPct: 90, watermarkLow: 70, watermarkHigh: 70, expected: 0},

		// High not yet reached..
		{total: 1000, free: 250, quotaPct: 100, watermarkLow: 50, watermarkHigh: 90, expected: 0},
		{total: 1000, free: 250, quotaPct: 100, watermarkLow: 50, watermarkHigh: 90, expected: 0},
	}
	for i, tc := range testCases {
		toClear := bytesToClear(tc.total, tc.free, tc.quotaPct, tc.watermarkLow, tc.watermarkHigh)
		if tc.expected != toClear {
			t.Errorf("test %d expected %v, got %v", i, tc.expected, toClear)
		}
	}
}
