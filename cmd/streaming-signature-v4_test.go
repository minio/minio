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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Test read chunk line.
func TestReadChunkLine(t *testing.T) {
	type testCase struct {
		reader         *bufio.Reader
		expectedErr    error
		chunkSize      []byte
		chunkSignature []byte
	}
	// List of readers used.
	readers := []io.Reader{
		// Test - 1
		bytes.NewReader([]byte("1000;chunk-signature=111123333333333333334444211\r\n")),
		// Test - 2
		bytes.NewReader([]byte("1000;")),
		// Test - 3
		bytes.NewReader(fmt.Appendf(nil, "%4097d", 1)),
		// Test - 4
		bytes.NewReader([]byte("1000;chunk-signature=111123333333333333334444211\r\n")),
	}
	testCases := []testCase{
		// Test - 1 - small bufio reader.
		{
			bufio.NewReaderSize(readers[0], 16),
			errLineTooLong,
			nil,
			nil,
		},
		// Test - 2 - unexpected end of the reader.
		{
			bufio.NewReader(readers[1]),
			io.ErrUnexpectedEOF,
			nil,
			nil,
		},
		// Test - 3 - line too long bigger than 4k+1
		{
			bufio.NewReader(readers[2]),
			errLineTooLong,
			nil,
			nil,
		},
		// Test - 4 - parse the chunk reader properly.
		{
			bufio.NewReader(readers[3]),
			nil,
			[]byte("1000"),
			[]byte("111123333333333333334444211"),
		},
	}
	// Valid test cases for each chunk line.
	for i, tt := range testCases {
		chunkSize, chunkSignature, err := readChunkLine(tt.reader)
		if err != tt.expectedErr {
			t.Errorf("Test %d: Expected %s, got %s", i+1, tt.expectedErr, err)
		}
		if !bytes.Equal(chunkSize, tt.chunkSize) {
			t.Errorf("Test %d: Expected %s, got %s", i+1, string(tt.chunkSize), string(chunkSize))
		}
		if !bytes.Equal(chunkSignature, tt.chunkSignature) {
			t.Errorf("Test %d: Expected %s, got %s", i+1, string(tt.chunkSignature), string(chunkSignature))
		}
	}
}

// Test parsing s3 chunk extension.
func TestParseS3ChunkExtension(t *testing.T) {
	type testCase struct {
		buf       []byte
		chunkSize []byte
		chunkSign []byte
	}

	tests := []testCase{
		// Test - 1 valid case.
		{
			[]byte("10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"),
			[]byte("10000"),
			[]byte("ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"),
		},
		// Test - 2 no chunk extension, return same buffer.
		{
			[]byte("10000;"),
			[]byte("10000;"),
			nil,
		},
		// Test - 3 no chunk size, return error.
		{
			[]byte(";chunk-signature="),
			nil,
			nil,
		},
		// Test - 4 removes trailing slash.
		{
			[]byte("10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648    \t \n"),
			[]byte("10000"),
			[]byte("ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"),
		},
	}
	// Validate chunk extension removal.
	for i, tt := range tests {
		// Extract chunk size and chunk signature after parsing a standard chunk-extension format.
		hexChunkSize, hexChunkSignature := parseS3ChunkExtension(tt.buf)
		if !bytes.Equal(hexChunkSize, tt.chunkSize) {
			t.Errorf("Test %d: Expected %s, got %s", i+1, string(tt.chunkSize), string(hexChunkSize))
		}
		if !bytes.Equal(hexChunkSignature, tt.chunkSign) {
			t.Errorf("Test %d: Expected %s, got %s", i+1, string(tt.chunkSign), string(hexChunkSignature))
		}
	}
}

// Test read CRLF characters on input reader.
func TestReadCRLF(t *testing.T) {
	type testCase struct {
		reader      io.Reader
		expectedErr error
	}
	tests := []testCase{
		// Test - 1 valid buffer with CRLF.
		{bytes.NewReader([]byte("\r\n")), nil},
		// Test - 2 invalid buffer with no CRLF.
		{bytes.NewReader([]byte("he")), errMalformedEncoding},
		// Test - 3 invalid buffer with more characters.
		{bytes.NewReader([]byte("he\r\n")), errMalformedEncoding},
		// Test - 4 smaller buffer than expected.
		{bytes.NewReader([]byte("h")), io.ErrUnexpectedEOF},
	}
	for i, tt := range tests {
		err := readCRLF(tt.reader)
		if err != tt.expectedErr {
			t.Errorf("Test %d: Expected %s, got %s this", i+1, tt.expectedErr, err)
		}
	}
}

// Tests parsing hex number into its uint64 decimal equivalent.
func TestParseHexUint(t *testing.T) {
	type testCase struct {
		in      string
		want    uint64
		wantErr string
	}
	tests := []testCase{
		{"x", 0, "invalid byte in chunk length"},
		{"0000000000000000", 0, ""},
		{"0000000000000001", 1, ""},
		{"ffffffffffffffff", 1<<64 - 1, ""},
		{"FFFFFFFFFFFFFFFF", 1<<64 - 1, ""},
		{"000000000000bogus", 0, "invalid byte in chunk length"},
		{"00000000000000000", 0, "http chunk length too large"}, // could accept if we wanted
		{"10000000000000000", 0, "http chunk length too large"},
		{"00000000000000001", 0, "http chunk length too large"}, // could accept if we wanted
	}
	for i := uint64(0); i <= 1234; i++ {
		tests = append(tests, testCase{in: fmt.Sprintf("%x", i), want: i})
	}
	for _, tt := range tests {
		got, err := parseHexUint([]byte(tt.in))
		if tt.wantErr != "" {
			if err != nil && !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("parseHexUint(%q) = %v, %v; want error %q", tt.in, got, err, tt.wantErr)
			}
		} else {
			if err != nil || got != tt.want {
				t.Errorf("parseHexUint(%q) = %v, %v; want %v", tt.in, got, err, tt.want)
			}
		}
	}
}
