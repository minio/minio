// Copyright (c) 2015-2025 MinIO, Inc.
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
	"net/http/httptest"
	"testing"
)

// TestChecksumAddToHeader tests that adding and retrieving a checksum on a header works
func TestChecksumAddToHeader(t *testing.T) {
	tests := []struct {
		name     string
		checksum ChecksumType
		fullobj  bool
	}{
		{"CRC32-composite", ChecksumCRC32, false},
		{"CRC32-full-object", ChecksumCRC32, true},
		{"CRC32C-composite", ChecksumCRC32C, false},
		{"CRC32C-full-object", ChecksumCRC32C, true},
		{"CRC64NVME-full-object", ChecksumCRC64NVME, false}, // testing with false, because it always is full object.
		{"ChecksumSHA1-composite", ChecksumSHA1, false},
		{"ChecksumSHA256-composite", ChecksumSHA256, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			myData := []byte("this-is-a-checksum-data-test")
			chksm := NewChecksumFromData(tt.checksum, myData)
			if tt.fullobj {
				chksm.Type |= ChecksumFullObject
			}

			w := httptest.NewRecorder()
			AddChecksumHeader(w, chksm.AsMap())
			gotChksm, err := GetContentChecksum(w.Result().Header)
			if err != nil {
				t.Fatalf("GetContentChecksum failed: %v", err)
			}

			// In the CRC64NVM case, it is always full object, so add the flag for easier equality comparison
			if chksm.Type.Base().Is(ChecksumCRC64NVME) {
				chksm.Type |= ChecksumFullObject
			}
			if !chksm.Equal(gotChksm) {
				t.Fatalf("Checksum mismatch: expected %+v, got %+v", chksm, gotChksm)
			}
		})
	}
}

// TestChecksumSerializeDeserialize checks AppendTo can be reversed by ChecksumFromBytes
func TestChecksumSerializeDeserialize(t *testing.T) {
	myData := []byte("this-is-a-checksum-data-test")
	chksm := NewChecksumFromData(ChecksumCRC32, myData)
	if chksm == nil {
		t.Fatal("NewChecksumFromData returned nil")
	}
	// Serialize the checksum to bytes
	b := chksm.AppendTo(nil, nil)
	if b == nil {
		t.Fatal("AppendTo returned nil")
	}

	// Deserialize the checksum from bytes
	chksmOut := ChecksumFromBytes(b)
	if chksmOut == nil {
		t.Fatal("ChecksumFromBytes returned nil")
	}

	// Assert new checksum matches the content
	matchError := chksmOut.Matches(myData, 0)
	if matchError != nil {
		t.Fatalf("Checksum mismatch on chksmOut: %v", matchError)
	}

	// Assert they are exactly equal
	if !chksmOut.Equal(chksm) {
		t.Fatalf("Checksum mismatch: expected %+v, got %+v", chksm, chksmOut)
	}
}

// TestChecksumSerializeDeserializeMultiPart checks AppendTo can be reversed by ChecksumFromBytes
// for multipart checksum
func TestChecksumSerializeDeserializeMultiPart(t *testing.T) {
	// Create dummy data that we'll split into 3 parts
	dummyData := []byte("The quick brown fox jumps over the lazy dog. " +
		"Pack my box with five dozen brown eggs. " +
		"Have another go it will all make sense in the end!")

	// Split data into 3 parts
	partSize := len(dummyData) / 3
	part1Data := dummyData[0:partSize]
	part2Data := dummyData[partSize : 2*partSize]
	part3Data := dummyData[2*partSize:]

	// Calculate CRC32C checksum for each part using NewChecksumFromData
	checksumType := ChecksumCRC32C

	part1Checksum := NewChecksumFromData(checksumType, part1Data)
	part2Checksum := NewChecksumFromData(checksumType, part2Data)
	part3Checksum := NewChecksumFromData(checksumType, part3Data)

	// Combine the raw checksums (this is what happens in CompleteMultipartUpload)
	var checksumCombined []byte
	checksumCombined = append(checksumCombined, part1Checksum.Raw...)
	checksumCombined = append(checksumCombined, part2Checksum.Raw...)
	checksumCombined = append(checksumCombined, part3Checksum.Raw...)

	// Create the final checksum (checksum of the combined checksums)
	// Add BOTH the multipart flag AND the includes-multipart flag
	finalChecksumType := checksumType | ChecksumMultipart | ChecksumIncludesMultipart
	finalChecksum := NewChecksumFromData(finalChecksumType, checksumCombined)

	// Set WantParts to indicate 3 parts
	finalChecksum.WantParts = 3

	// Test AppendTo serialization
	var serialized []byte
	serialized = finalChecksum.AppendTo(serialized, checksumCombined)

	// Use ChecksumFromBytes to deserialize the final checksum
	chksmOut := ChecksumFromBytes(serialized)
	if chksmOut == nil {
		t.Fatal("ChecksumFromBytes returned nil")
	}

	// Assert they are exactly equal
	if !chksmOut.Equal(finalChecksum) {
		t.Fatalf("Checksum mismatch: expected %+v, got %+v", finalChecksum, chksmOut)
	}

	// Serialize what we got from ChecksumFromBytes
	serializedOut := chksmOut.AppendTo(nil, checksumCombined)

	// Read part checksums from serializedOut
	readParts := ReadPartCheckSums(serializedOut)
	expectedChecksums := []string{
		part1Checksum.Encoded,
		part2Checksum.Encoded,
		part3Checksum.Encoded,
	}
	for i, expected := range expectedChecksums {
		if got := readParts[i][ChecksumCRC32C.String()]; got != expected {
			t.Fatalf("want part%dChecksum.Encoded %s, got %s", i+1, expected, got)
		}
	}
}
