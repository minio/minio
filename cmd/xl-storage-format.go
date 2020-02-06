/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"context"

	jsoniter "github.com/json-iterator/go"
)

// XL constants.
const (
	// XL metadata file carries per object metadata.
	xlStorageFormatFile = "xl.json"
)

// IsValid - tells if the format is sane by validating the version
// string, format and erasure info fields.
func (m xlMetaV1) IsValid() bool {
	return isXLMetaFormatValid(m.Version, m.Format) &&
		isXLMetaErasureInfoValid(m.Erasure.DataBlocks, m.Erasure.ParityBlocks)
}

// Verifies if the backend format metadata is sane by validating
// the version string and format style.
func isXLMetaFormatValid(version, format string) bool {
	return ((version == xlMetaVersion || version == xlMetaVersion100) &&
		format == xlMetaFormat)
}

// Verifies if the backend format metadata is sane by validating
// the ErasureInfo, i.e. data and parity blocks.
func isXLMetaErasureInfoValid(data, parity int) bool {
	return ((data >= parity) && (data != 0) && (parity != 0))
}

// A xlMetaV1 represents `xl.json` metadata header.
type xlMetaV1 struct {
	Version string   `json:"version"` // Version of the current `xl.json`.
	Format  string   `json:"format"`  // Format of the current `xl.json`.
	Stat    StatInfo `json:"stat"`    // Stat of the current object `xl.json`.
	// Erasure coded info for the current object `xl.json`.
	Erasure ErasureInfo `json:"erasure"`
	// MinIO release tag for current object `xl.json`.
	Minio struct {
		Release string `json:"release"`
	} `json:"minio"`
	// Metadata map for current object `xl.json`.
	Meta map[string]string `json:"meta,omitempty"`
	// Captures all the individual object `xl.json`.
	Parts []ObjectPartInfo `json:"parts,omitempty"`
}

// XL metadata constants.
const (
	// XL meta version.
	xlMetaVersion = "1.0.1"

	// XL meta version.
	xlMetaVersion100 = "1.0.0"

	// XL meta format string.
	xlMetaFormat = "xl"

	// Add new constants here.
)

// newFileInfo - initializes new FileInfo, allocates a fresh erasure info.
func newFileInfo(object string, dataBlocks, parityBlocks int) (fi FileInfo) {
	meta := newXLMetaV1(object, dataBlocks, parityBlocks)
	fi.Erasure = meta.Erasure
	return fi
}

// newXLMetaV1 - initializes new xlMetaV1, adds version, allocates a fresh erasure info.
func newXLMetaV1(object string, dataBlocks, parityBlocks int) (xlMeta xlMetaV1) {
	xlMeta = xlMetaV1{}
	xlMeta.Version = xlMetaVersion
	xlMeta.Format = xlMetaFormat
	xlMeta.Minio.Release = ReleaseTag
	xlMeta.Erasure = ErasureInfo{
		Algorithm:    erasureAlgorithmKlauspost,
		DataBlocks:   dataBlocks,
		ParityBlocks: parityBlocks,
		BlockSize:    blockSizeV1,
		Distribution: hashOrder(object, dataBlocks+parityBlocks),
	}
	return xlMeta
}

// Constructs xlMetaV1 using `jsoniter` lib.
func xlMetaV1UnmarshalJSON(ctx context.Context, xlMetaBuf []byte) (xlMeta xlMetaV1, err error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	err = json.Unmarshal(xlMetaBuf, &xlMeta)
	return xlMeta, err
}
