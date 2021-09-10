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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/logger"
)

// XL constants.
const (
	// XL metadata file carries per object metadata.
	xlStorageFormatFileV1 = "xl.json"
)

// Valid - tells us if the format is sane by validating
// format version and erasure coding information.
func (m *xlMetaV1Object) valid() bool {
	return isXLMetaFormatValid(m.Version, m.Format) &&
		isXLMetaErasureInfoValid(m.Erasure.DataBlocks, m.Erasure.ParityBlocks)
}

// Verifies if the backend format metadata is sane by validating
// the version string and format style.
func isXLMetaFormatValid(version, format string) bool {
	return ((version == xlMetaVersion101 ||
		version == xlMetaVersion100) &&
		format == xlMetaFormat)
}

// Verifies if the backend format metadata is sane by validating
// the ErasureInfo, i.e. data and parity blocks.
func isXLMetaErasureInfoValid(data, parity int) bool {
	return ((data >= parity) && (data != 0) && (parity != 0))
}

//go:generate msgp -file=$GOFILE -unexported

// A xlMetaV1Object represents `xl.meta` metadata header.
type xlMetaV1Object struct {
	Version string   `json:"version"` // Version of the current `xl.meta`.
	Format  string   `json:"format"`  // Format of the current `xl.meta`.
	Stat    StatInfo `json:"stat"`    // Stat of the current object `xl.meta`.
	// Erasure coded info for the current object `xl.meta`.
	Erasure ErasureInfo `json:"erasure"`
	// MinIO release tag for current object `xl.meta`.
	Minio struct {
		Release string `json:"release"`
	} `json:"minio"`
	// Metadata map for current object `xl.meta`.
	Meta map[string]string `json:"meta,omitempty"`
	// Captures all the individual object `xl.meta`.
	Parts []ObjectPartInfo `json:"parts,omitempty"`

	// Dummy values used for legacy use cases.
	VersionID string `json:"versionId,omitempty"`
	DataDir   string `json:"dataDir,omitempty"` // always points to "legacy"
}

// StatInfo - carries stat information of the object.
type StatInfo struct {
	Size    int64     `json:"size"`    // Size of the object `xl.meta`.
	ModTime time.Time `json:"modTime"` // ModTime of the object `xl.meta`.
}

// ErasureInfo holds erasure coding and bitrot related information.
type ErasureInfo struct {
	// Algorithm is the string representation of erasure-coding-algorithm
	Algorithm string `json:"algorithm"`
	// DataBlocks is the number of data blocks for erasure-coding
	DataBlocks int `json:"data"`
	// ParityBlocks is the number of parity blocks for erasure-coding
	ParityBlocks int `json:"parity"`
	// BlockSize is the size of one erasure-coded block
	BlockSize int64 `json:"blockSize"`
	// Index is the index of the current disk
	Index int `json:"index"`
	// Distribution is the distribution of the data and parity blocks
	Distribution []int `json:"distribution"`
	// Checksums holds all bitrot checksums of all erasure encoded blocks
	Checksums []ChecksumInfo `json:"checksum,omitempty"`
}

// BitrotAlgorithm specifies a algorithm used for bitrot protection.
type BitrotAlgorithm uint

const (
	// SHA256 represents the SHA-256 hash function
	SHA256 BitrotAlgorithm = 1 + iota
	// HighwayHash256 represents the HighwayHash-256 hash function
	HighwayHash256
	// HighwayHash256S represents the Streaming HighwayHash-256 hash function
	HighwayHash256S
	// BLAKE2b512 represents the BLAKE2b-512 hash function
	BLAKE2b512
)

// DefaultBitrotAlgorithm is the default algorithm used for bitrot protection.
const (
	DefaultBitrotAlgorithm = HighwayHash256S
)

// ObjectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type ObjectPartInfo struct {
	ETag       string `json:"etag,omitempty"`
	Number     int    `json:"number"`
	Size       int64  `json:"size"`
	ActualSize int64  `json:"actualSize"`
}

// ChecksumInfo - carries checksums of individual scattered parts per disk.
type ChecksumInfo struct {
	PartNumber int
	Algorithm  BitrotAlgorithm
	Hash       []byte
}

type checksumInfoJSON struct {
	Name      string `json:"name"`
	Algorithm string `json:"algorithm"`
	Hash      string `json:"hash,omitempty"`
}

// MarshalJSON marshals the ChecksumInfo struct
func (c ChecksumInfo) MarshalJSON() ([]byte, error) {
	info := checksumInfoJSON{
		Name:      fmt.Sprintf("part.%d", c.PartNumber),
		Algorithm: c.Algorithm.String(),
		Hash:      hex.EncodeToString(c.Hash),
	}
	return json.Marshal(info)
}

// UnmarshalJSON - custom checksum info unmarshaller
func (c *ChecksumInfo) UnmarshalJSON(data []byte) error {
	var info checksumInfoJSON
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	sum, err := hex.DecodeString(info.Hash)
	if err != nil {
		return err
	}
	c.Algorithm = BitrotAlgorithmFromString(info.Algorithm)
	c.Hash = sum
	if _, err = fmt.Sscanf(info.Name, "part.%d", &c.PartNumber); err != nil {
		return err
	}

	if !c.Algorithm.Available() {
		logger.LogIf(GlobalContext, errBitrotHashAlgoInvalid)
		return errBitrotHashAlgoInvalid
	}
	return nil
}

// constant and shouldn't be changed.
const (
	legacyDataDir = "legacy"
)

func (m *xlMetaV1Object) ToFileInfo(volume, path string) (FileInfo, error) {
	if !m.valid() {
		return FileInfo{}, errFileCorrupt
	}

	fi := FileInfo{
		Volume:    volume,
		Name:      path,
		ModTime:   m.Stat.ModTime,
		Size:      m.Stat.Size,
		Metadata:  m.Meta,
		Parts:     m.Parts,
		Erasure:   m.Erasure,
		VersionID: m.VersionID,
		DataDir:   m.DataDir,
	}
	return fi, nil
}

// XL metadata constants.
const (
	// XL meta version.
	xlMetaVersion101 = "1.0.1"

	// XL meta version.
	xlMetaVersion100 = "1.0.0"

	// XL meta format string.
	xlMetaFormat = "xl"
)
