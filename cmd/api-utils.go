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
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"strings"
)

func shouldEscape(c byte) bool {
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' {
		return false
	}

	switch c {
	case '-', '_', '.', '/', '*':
		return false
	}
	return true
}

// s3URLEncode is based on Golang's url.QueryEscape() code,
// while considering some S3 exceptions:
//   - Avoid encoding '/' and '*'
//   - Force encoding of '~'
func s3URLEncode(s string) string {
	spaceCount, hexCount := 0, 0
	for i := range len(s) {
		c := s[i]
		if shouldEscape(c) {
			if c == ' ' {
				spaceCount++
			} else {
				hexCount++
			}
		}
	}

	if spaceCount == 0 && hexCount == 0 {
		return s
	}

	var buf [64]byte
	var t []byte

	required := len(s) + 2*hexCount
	if required <= len(buf) {
		t = buf[:required]
	} else {
		t = make([]byte, required)
	}

	if hexCount == 0 {
		copy(t, s)
		for i := range len(s) {
			if s[i] == ' ' {
				t[i] = '+'
			}
		}
		return string(t)
	}

	j := 0
	for i := range len(s) {
		switch c := s[i]; {
		case c == ' ':
			t[j] = '+'
			j++
		case shouldEscape(c):
			t[j] = '%'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}

// s3EncodeName encodes string in response when encodingType is specified in AWS S3 requests.
func s3EncodeName(name, encodingType string) string {
	if strings.ToLower(encodingType) == "url" {
		return s3URLEncode(name)
	}
	return name
}

// getHandlerName returns the name of the handler function. It takes the type
// name as a string to clean up the name retrieved via reflection. This function
// only works correctly when the type is present in the cmd package.
func getHandlerName(f http.HandlerFunc, cmdType string) string {
	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()

	packageName := fmt.Sprintf("github.com/minio/minio/cmd.%s.", cmdType)
	name = strings.TrimPrefix(name, packageName)
	name = strings.TrimSuffix(name, "Handler-fm")
	name = strings.TrimSuffix(name, "-fm")
	return name
}
