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

package cmd

import (
	"net/url"
	"testing"
)

// Test list objects resources V2.
func TestListObjectsV2Resources(t *testing.T) {
	testCases := []struct {
		values                               url.Values
		prefix, token, startAfter, delimiter string
		fetchOwner                           bool
		maxKeys                              int
		encodingType                         string
		errCode                              APIErrorCode
	}{
		{
			values: url.Values{
				"prefix":             []string{"photos/"},
				"continuation-token": []string{"dG9rZW4="},
				"start-after":        []string{"start-after"},
				"delimiter":          []string{SlashSeparator},
				"fetch-owner":        []string{"true"},
				"max-keys":           []string{"100"},
				"encoding-type":      []string{"gzip"},
			},
			prefix:       "photos/",
			token:        "token",
			startAfter:   "start-after",
			delimiter:    SlashSeparator,
			fetchOwner:   true,
			maxKeys:      100,
			encodingType: "gzip",
			errCode:      ErrNone,
		},
		{
			values: url.Values{
				"prefix":             []string{"photos/"},
				"continuation-token": []string{"dG9rZW4="},
				"start-after":        []string{"start-after"},
				"delimiter":          []string{SlashSeparator},
				"fetch-owner":        []string{"true"},
				"encoding-type":      []string{"gzip"},
			},
			prefix:       "photos/",
			token:        "token",
			startAfter:   "start-after",
			delimiter:    SlashSeparator,
			fetchOwner:   true,
			maxKeys:      maxObjectList,
			encodingType: "gzip",
			errCode:      ErrNone,
		},
		{
			values: url.Values{
				"prefix":             []string{"photos/"},
				"continuation-token": []string{""},
				"start-after":        []string{"start-after"},
				"delimiter":          []string{SlashSeparator},
				"fetch-owner":        []string{"true"},
				"encoding-type":      []string{"gzip"},
			},
			prefix:       "",
			token:        "",
			startAfter:   "",
			delimiter:    "",
			fetchOwner:   false,
			maxKeys:      0,
			encodingType: "",
			errCode:      ErrIncorrectContinuationToken,
		},
	}

	for i, testCase := range testCases {
		prefix, token, startAfter, delimiter, fetchOwner, maxKeys, encodingType, errCode := getListObjectsV2Args(testCase.values)

		if errCode != testCase.errCode {
			t.Errorf("Test %d: Expected error code:%d, got %d", i+1, testCase.errCode, errCode)
		}
		if prefix != testCase.prefix {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.prefix, prefix)
		}
		if token != testCase.token {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.token, token)
		}
		if startAfter != testCase.startAfter {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.startAfter, startAfter)
		}
		if delimiter != testCase.delimiter {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.delimiter, delimiter)
		}
		if fetchOwner != testCase.fetchOwner {
			t.Errorf("Test %d: Expected %t, got %t", i+1, testCase.fetchOwner, fetchOwner)
		}
		if maxKeys != testCase.maxKeys {
			t.Errorf("Test %d: Expected %d, got %d", i+1, testCase.maxKeys, maxKeys)
		}
		if encodingType != testCase.encodingType {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.encodingType, encodingType)
		}
	}
}

// Test list objects resources V1.
func TestListObjectsV1Resources(t *testing.T) {
	testCases := []struct {
		values                    url.Values
		prefix, marker, delimiter string
		maxKeys                   int
		encodingType              string
	}{
		{
			values: url.Values{
				"prefix":        []string{"photos/"},
				"marker":        []string{"test"},
				"delimiter":     []string{SlashSeparator},
				"max-keys":      []string{"100"},
				"encoding-type": []string{"gzip"},
			},
			prefix:       "photos/",
			marker:       "test",
			delimiter:    SlashSeparator,
			maxKeys:      100,
			encodingType: "gzip",
		},
		{
			values: url.Values{
				"prefix":        []string{"photos/"},
				"marker":        []string{"test"},
				"delimiter":     []string{SlashSeparator},
				"encoding-type": []string{"gzip"},
			},
			prefix:       "photos/",
			marker:       "test",
			delimiter:    SlashSeparator,
			maxKeys:      maxObjectList,
			encodingType: "gzip",
		},
	}

	for i, testCase := range testCases {
		prefix, marker, delimiter, maxKeys, encodingType, argsErr := getListObjectsV1Args(testCase.values)
		if argsErr != ErrNone {
			t.Errorf("Test %d: argument parsing failed, got %v", i+1, argsErr)
		}
		if prefix != testCase.prefix {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.prefix, prefix)
		}
		if marker != testCase.marker {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.marker, marker)
		}
		if delimiter != testCase.delimiter {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.delimiter, delimiter)
		}
		if maxKeys != testCase.maxKeys {
			t.Errorf("Test %d: Expected %d, got %d", i+1, testCase.maxKeys, maxKeys)
		}
		if encodingType != testCase.encodingType {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.encodingType, encodingType)
		}
	}
}

// Validates extracting information for object resources.
func TestGetObjectsResources(t *testing.T) {
	testCases := []struct {
		values                     url.Values
		uploadID                   string
		partNumberMarker, maxParts int
		encodingType               string
	}{
		{
			values: url.Values{
				"uploadId":           []string{"11123-11312312311231-12313"},
				"part-number-marker": []string{"1"},
				"max-parts":          []string{"1000"},
				"encoding-type":      []string{"gzip"},
			},
			uploadID:         "11123-11312312311231-12313",
			partNumberMarker: 1,
			maxParts:         1000,
			encodingType:     "gzip",
		},
	}

	for i, testCase := range testCases {
		uploadID, partNumberMarker, maxParts, encodingType, argsErr := getObjectResources(testCase.values)
		if argsErr != ErrNone {
			t.Errorf("Test %d: argument parsing failed, got %v", i+1, argsErr)
		}
		if uploadID != testCase.uploadID {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.uploadID, uploadID)
		}
		if partNumberMarker != testCase.partNumberMarker {
			t.Errorf("Test %d: Expected %d, got %d", i+1, testCase.partNumberMarker, partNumberMarker)
		}
		if maxParts != testCase.maxParts {
			t.Errorf("Test %d: Expected %d, got %d", i+1, testCase.maxParts, maxParts)
		}
		if encodingType != testCase.encodingType {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.encodingType, encodingType)
		}
	}
}
