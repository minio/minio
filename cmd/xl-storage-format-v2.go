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
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
)

var (
	// XL header specifies the format
	xlHeader = [4]byte{'X', 'L', '2', ' '}

	// XLv2 version 1
	xlVersionV1 = [4]byte{'1', ' ', ' ', ' '}
)

func checkXL2V1(buf []byte) error {
	if len(buf) <= 8 {
		return fmt.Errorf("xlMeta: no data")
	}

	if !bytes.Equal(buf[:4], xlHeader[:]) {
		return fmt.Errorf("xlMeta: unknown XLv2 header, expected %v, got %v", xlHeader[:4], buf[:4])
	}

	if !bytes.Equal(buf[4:8], xlVersionV1[:]) {
		return fmt.Errorf("xlMeta: unknown XLv2 version, expected %v, got %v", xlVersionV1[:4], buf[4:8])
	}

	return nil
}

func isXL2V1Format(buf []byte) bool {
	return checkXL2V1(buf) == nil
}

// The []journal contains all the different versions of the object.
//
// This array can have 3 kinds of objects:
//
// ``object``: If the object is uploaded the usual way: putobject, multipart-put, copyobject
//
// ``delete``: This is the delete-marker
//
// ``legacyObject``: This is the legacy object in xlV1 format, preserved until its overwritten
//
// The most recently updated element in the array is considered the latest version.

// Backend directory tree structure:
// disk1/
// └── bucket
//     └── object
//         ├── a192c1d5-9bd5-41fd-9a90-ab10e165398d
//         │   └── part.1
//         ├── c06e0436-f813-447e-ae5e-f2564df9dfd4
//         │   └── part.1
//         ├── df433928-2dcf-47b1-a786-43efa0f6b424
//         │   └── part.1
//         ├── legacy
//         │   └── part.1
//         └── xl.meta

//go:generate msgp -file=$GOFILE -unexported

// VersionType defines the type of journal type of the current entry.
type VersionType uint8

// List of different types of journal type
const (
	invalidVersionType VersionType = 0
	ObjectType         VersionType = 1
	DeleteType         VersionType = 2
	LegacyType         VersionType = 3
	lastVersionType    VersionType = 4
)

func (e VersionType) valid() bool {
	return e > invalidVersionType && e < lastVersionType
}

// ErasureAlgo defines common type of different erasure algorithms
type ErasureAlgo uint8

// List of currently supported erasure coding algorithms
const (
	invalidErasureAlgo ErasureAlgo = 0
	ReedSolomon        ErasureAlgo = 1
	lastErasureAlgo    ErasureAlgo = 2
)

func (e ErasureAlgo) valid() bool {
	return e > invalidErasureAlgo && e < lastErasureAlgo
}

func (e ErasureAlgo) String() string {
	switch e {
	case ReedSolomon:
		return "reedsolomon"
	}
	return ""
}

// ChecksumAlgo defines common type of different checksum algorithms
type ChecksumAlgo uint8

// List of currently supported checksum algorithms
const (
	invalidChecksumAlgo ChecksumAlgo = 0
	HighwayHash         ChecksumAlgo = 1
	lastChecksumAlgo    ChecksumAlgo = 2
)

func (e ChecksumAlgo) valid() bool {
	return e > invalidChecksumAlgo && e < lastChecksumAlgo
}

// xlMetaV2DeleteMarker defines the data struct for the delete marker journal type
type xlMetaV2DeleteMarker struct {
	VersionID [16]byte `json:"ID" msg:"ID"`       // Version ID for delete marker
	ModTime   int64    `json:"MTime" msg:"MTime"` // Object delete marker modified time
}

// xlMetaV2Object defines the data struct for object journal type
type xlMetaV2Object struct {
	VersionID          [16]byte          `json:"ID" msg:"ID"`                                     // Version ID
	DataDir            [16]byte          `json:"DDir" msg:"DDir"`                                 // Data dir ID
	ErasureAlgorithm   ErasureAlgo       `json:"EcAlgo" msg:"EcAlgo"`                             // Erasure coding algorithm
	ErasureM           int               `json:"EcM" msg:"EcM"`                                   // Erasure data blocks
	ErasureN           int               `json:"EcN" msg:"EcN"`                                   // Erasure parity blocks
	ErasureBlockSize   int64             `json:"EcBSize" msg:"EcBSize"`                           // Erasure block size
	ErasureIndex       int               `json:"EcIndex" msg:"EcIndex"`                           // Erasure disk index
	ErasureDist        []uint8           `json:"EcDist" msg:"EcDist"`                             // Erasure distribution
	BitrotChecksumAlgo ChecksumAlgo      `json:"CSumAlgo" msg:"CSumAlgo"`                         // Bitrot checksum algo
	PartNumbers        []int             `json:"PartNums" msg:"PartNums"`                         // Part Numbers
	PartETags          []string          `json:"PartETags" msg:"PartETags"`                       // Part ETags
	PartSizes          []int64           `json:"PartSizes" msg:"PartSizes"`                       // Part Sizes
	PartActualSizes    []int64           `json:"PartASizes,omitempty" msg:"PartASizes,omitempty"` // Part ActualSizes (compression)
	Size               int64             `json:"Size" msg:"Size"`                                 // Object version size
	ModTime            int64             `json:"MTime" msg:"MTime"`                               // Object version modified time
	MetaSys            map[string][]byte `json:"MetaSys,omitempty" msg:"MetaSys,omitempty"`       // Object version internal metadata
	MetaUser           map[string]string `json:"MetaUsr,omitempty" msg:"MetaUsr,omitempty"`       // Object version metadata set by user
}

// xlMetaV2Version describes the jouranal entry, Type defines
// the current journal entry type other types might be nil based
// on what Type field carries, it is imperative for the caller
// to verify which journal type first before accessing rest of the fields.
type xlMetaV2Version struct {
	Type         VersionType           `json:"Type" msg:"Type"`
	ObjectV1     *xlMetaV1Object       `json:"V1Obj,omitempty" msg:"V1Obj,omitempty"`
	ObjectV2     *xlMetaV2Object       `json:"V2Obj,omitempty" msg:"V2Obj,omitempty"`
	DeleteMarker *xlMetaV2DeleteMarker `json:"DelObj,omitempty" msg:"DelObj,omitempty"`
}

// Valid xl meta xlMetaV2Version is valid
func (j xlMetaV2Version) Valid() bool {
	switch j.Type {
	case LegacyType:
		return j.ObjectV1 != nil &&
			j.ObjectV1.valid()
	case ObjectType:
		return j.ObjectV2 != nil &&
			j.ObjectV2.ErasureAlgorithm.valid() &&
			j.ObjectV2.BitrotChecksumAlgo.valid() &&
			isXLMetaErasureInfoValid(j.ObjectV2.ErasureM, j.ObjectV2.ErasureN) &&
			j.ObjectV2.ModTime > 0
	case DeleteType:
		return j.DeleteMarker != nil &&
			j.DeleteMarker.ModTime > 0
	}
	return false
}

// xlMetaV2 - object meta structure defines the format and list of
// the journals for the object.
type xlMetaV2 struct {
	Versions []xlMetaV2Version `json:"Versions" msg:"Versions"`
}

// AddLegacy adds a legacy version, is only called when no prior
// versions exist, safe to use it by only one function in xl-storage(RenameData)
func (z *xlMetaV2) AddLegacy(m *xlMetaV1Object) error {
	if !m.valid() {
		return errFileCorrupt
	}
	m.VersionID = nullVersionID
	m.DataDir = legacyDataDir
	z.Versions = []xlMetaV2Version{
		{
			Type:     LegacyType,
			ObjectV1: m,
		},
	}
	return nil
}

// Load unmarshal and load the entire message pack.
func (z *xlMetaV2) Load(buf []byte) error {
	if err := checkXL2V1(buf); err != nil {
		return err
	}
	_, err := z.UnmarshalMsg(buf[8:])
	return err
}

// AddVersion adds a new version
func (z *xlMetaV2) AddVersion(fi FileInfo) error {
	if fi.VersionID == "" {
		// this means versioning is not yet
		// enabled or suspend i.e all versions
		// are basically default value i.e "null"
		fi.VersionID = nullVersionID
	}

	var uv uuid.UUID
	var err error
	if fi.VersionID != "" && fi.VersionID != nullVersionID {
		uv, err = uuid.Parse(fi.VersionID)
		if err != nil {
			return err
		}
	}

	var dd uuid.UUID
	if fi.DataDir != "" {
		dd, err = uuid.Parse(fi.DataDir)
		if err != nil {
			return err
		}
	}

	ventry := xlMetaV2Version{}

	if fi.Deleted {
		ventry.Type = DeleteType
		ventry.DeleteMarker = &xlMetaV2DeleteMarker{
			VersionID: uv,
			ModTime:   fi.ModTime.UnixNano(),
		}
	} else {
		ventry.Type = ObjectType
		ventry.ObjectV2 = &xlMetaV2Object{
			VersionID:          uv,
			DataDir:            dd,
			Size:               fi.Size,
			ModTime:            fi.ModTime.UnixNano(),
			ErasureAlgorithm:   ReedSolomon,
			ErasureM:           fi.Erasure.DataBlocks,
			ErasureN:           fi.Erasure.ParityBlocks,
			ErasureBlockSize:   fi.Erasure.BlockSize,
			ErasureIndex:       fi.Erasure.Index,
			BitrotChecksumAlgo: HighwayHash,
			ErasureDist:        make([]uint8, len(fi.Erasure.Distribution)),
			PartNumbers:        make([]int, len(fi.Parts)),
			PartETags:          make([]string, len(fi.Parts)),
			PartSizes:          make([]int64, len(fi.Parts)),
			PartActualSizes:    make([]int64, len(fi.Parts)),
			MetaSys:            make(map[string][]byte),
			MetaUser:           make(map[string]string, len(fi.Metadata)),
		}

		for i := range fi.Erasure.Distribution {
			ventry.ObjectV2.ErasureDist[i] = uint8(fi.Erasure.Distribution[i])
		}

		for i := range fi.Parts {
			ventry.ObjectV2.PartSizes[i] = fi.Parts[i].Size
			if fi.Parts[i].ETag != "" {
				ventry.ObjectV2.PartETags[i] = fi.Parts[i].ETag
			}
			ventry.ObjectV2.PartNumbers[i] = fi.Parts[i].Number
			ventry.ObjectV2.PartActualSizes[i] = fi.Parts[i].ActualSize
		}

		for k, v := range fi.Metadata {
			if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
				ventry.ObjectV2.MetaSys[k] = []byte(v)
			} else {
				ventry.ObjectV2.MetaUser[k] = v
			}
		}
	}

	if !ventry.Valid() {
		return errors.New("internal error: invalid version entry generated")
	}

	for i, version := range z.Versions {
		if !version.Valid() {
			return errFileCorrupt
		}
		switch version.Type {
		case LegacyType:
			// This would convert legacy type into new ObjectType
			// this means that we are basically purging the `null`
			// version of the object.
			if version.ObjectV1.VersionID == fi.VersionID {
				z.Versions[i] = ventry
				return nil
			}
		case ObjectType:
			if bytes.Equal(version.ObjectV2.VersionID[:], uv[:]) {
				z.Versions[i] = ventry
				return nil
			}
		case DeleteType:
			// Allowing delete marker to replaced with an proper
			// object data type as well, this is not S3 complaint
			// behavior but kept here for future flexibility.
			if bytes.Equal(version.DeleteMarker.VersionID[:], uv[:]) {
				z.Versions[i] = ventry
				return nil
			}
		}
	}

	z.Versions = append(z.Versions, ventry)
	return nil
}

func newXLMetaV2(fi FileInfo) (xlMetaV2, error) {
	xlMeta := xlMetaV2{}
	return xlMeta, xlMeta.AddVersion(fi)
}

func (j xlMetaV2DeleteMarker) ToFileInfo(volume, path string) (FileInfo, error) {
	versionID := ""
	var uv uuid.UUID
	// check if the version is not "null"
	if !bytes.Equal(j.VersionID[:], uv[:]) {
		versionID = uuid.UUID(j.VersionID).String()
	}
	fi := FileInfo{
		Volume:    volume,
		Name:      path,
		ModTime:   time.Unix(0, j.ModTime).UTC(),
		VersionID: versionID,
		Deleted:   true,
	}
	return fi, nil
}

func (j xlMetaV2Object) ToFileInfo(volume, path string) (FileInfo, error) {
	versionID := ""
	var uv uuid.UUID
	// check if the version is not "null"
	if !bytes.Equal(j.VersionID[:], uv[:]) {
		versionID = uuid.UUID(j.VersionID).String()
	}
	fi := FileInfo{
		Volume:    volume,
		Name:      path,
		Size:      j.Size,
		ModTime:   time.Unix(0, j.ModTime).UTC(),
		VersionID: versionID,
	}
	fi.Parts = make([]ObjectPartInfo, len(j.PartNumbers))
	for i := range fi.Parts {
		fi.Parts[i].Number = j.PartNumbers[i]
		fi.Parts[i].Size = j.PartSizes[i]
		fi.Parts[i].ETag = j.PartETags[i]
		fi.Parts[i].ActualSize = j.PartActualSizes[i]
	}
	fi.Erasure.Checksums = make([]ChecksumInfo, len(j.PartSizes))
	for i := range fi.Parts {
		fi.Erasure.Checksums[i].PartNumber = fi.Parts[i].Number
		switch j.BitrotChecksumAlgo {
		case HighwayHash:
			fi.Erasure.Checksums[i].Algorithm = HighwayHash256S
			fi.Erasure.Checksums[i].Hash = []byte{}
		default:
			return FileInfo{}, fmt.Errorf("unknown BitrotChecksumAlgo: %v", j.BitrotChecksumAlgo)
		}
	}
	fi.Metadata = make(map[string]string, len(j.MetaUser)+len(j.MetaSys))
	for k, v := range j.MetaUser {
		// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
		if strings.EqualFold(k, xhttp.AmzMetaUnencryptedContentLength) || strings.EqualFold(k, xhttp.AmzMetaUnencryptedContentMD5) {
			continue
		}

		fi.Metadata[k] = v
	}
	for k, v := range j.MetaSys {
		if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
			fi.Metadata[k] = string(v)
		}
	}
	fi.Erasure.Algorithm = j.ErasureAlgorithm.String()
	fi.Erasure.Index = j.ErasureIndex
	fi.Erasure.BlockSize = j.ErasureBlockSize
	fi.Erasure.DataBlocks = j.ErasureM
	fi.Erasure.ParityBlocks = j.ErasureN
	fi.Erasure.Distribution = make([]int, len(j.ErasureDist))
	for i := range j.ErasureDist {
		fi.Erasure.Distribution[i] = int(j.ErasureDist[i])
	}
	fi.DataDir = uuid.UUID(j.DataDir).String()
	return fi, nil
}

// DeleteVersion deletes the version specified by version id.
// returns to the caller which dataDir to delete, also
// indicates if this is the last version.
func (z *xlMetaV2) DeleteVersion(fi FileInfo) (string, bool, error) {
	// This is a situation where versionId is explicitly
	// specified as "null", as we do not save "null"
	// string it is considered empty. But empty also
	// means the version which matches will be purged.
	if fi.VersionID == nullVersionID {
		fi.VersionID = ""
	}

	var uv uuid.UUID
	if fi.VersionID != "" {
		uv, _ = uuid.Parse(fi.VersionID)
	}

	var ventry xlMetaV2Version
	if fi.Deleted {
		ventry = xlMetaV2Version{
			Type: DeleteType,
			DeleteMarker: &xlMetaV2DeleteMarker{
				VersionID: uv,
				ModTime:   fi.ModTime.UnixNano(),
			},
		}
		if !ventry.Valid() {
			return "", false, errors.New("internal error: invalid version entry generated")
		}
	}

	for i, version := range z.Versions {
		if !version.Valid() {
			return "", false, errFileCorrupt
		}
		switch version.Type {
		case LegacyType:
			if version.ObjectV1.VersionID == fi.VersionID {
				z.Versions = append(z.Versions[:i], z.Versions[i+1:]...)
				if fi.Deleted {
					z.Versions = append(z.Versions, ventry)
				}
				return version.ObjectV1.DataDir, len(z.Versions) == 0, nil
			}
		case DeleteType:
			if bytes.Equal(version.DeleteMarker.VersionID[:], uv[:]) {
				z.Versions = append(z.Versions[:i], z.Versions[i+1:]...)
				if fi.Deleted {
					z.Versions = append(z.Versions, ventry)
				}
				return "", len(z.Versions) == 0, nil
			}
		}
	}

	findDataDir := func(dataDir [16]byte, versions []xlMetaV2Version) int {
		var sameDataDirCount int
		for _, version := range versions {
			switch version.Type {
			case ObjectType:
				if bytes.Equal(version.ObjectV2.DataDir[:], dataDir[:]) {
					sameDataDirCount++
				}
			}
		}
		return sameDataDirCount
	}

	for i, version := range z.Versions {
		if !version.Valid() {
			return "", false, errFileCorrupt
		}
		switch version.Type {
		case ObjectType:
			if bytes.Equal(version.ObjectV2.VersionID[:], uv[:]) {
				z.Versions = append(z.Versions[:i], z.Versions[i+1:]...)
				if findDataDir(version.ObjectV2.DataDir, z.Versions) > 0 {
					if fi.Deleted {
						z.Versions = append(z.Versions, ventry)
					}
					// Found that another version references the same dataDir
					// we shouldn't remove it, and only remove the version instead
					return "", len(z.Versions) == 0, nil
				}
				if fi.Deleted {
					z.Versions = append(z.Versions, ventry)
				}
				return uuid.UUID(version.ObjectV2.DataDir).String(), len(z.Versions) == 0, nil
			}
		}
	}

	if fi.Deleted {
		z.Versions = append(z.Versions, ventry)
		return "", false, nil
	}

	return "", false, errFileVersionNotFound
}

// TotalSize returns the total size of all versions.
func (z xlMetaV2) TotalSize() int64 {
	var total int64
	for i := range z.Versions {
		switch z.Versions[i].Type {
		case ObjectType:
			total += z.Versions[i].ObjectV2.Size
		case LegacyType:
			total += z.Versions[i].ObjectV1.Stat.Size
		}
	}
	return total
}

// ListVersions lists current versions, and current deleted
// versions returns error for unexpected entries.
func (z xlMetaV2) ListVersions(volume, path string) (versions []FileInfo, modTime time.Time, err error) {
	var latestModTime time.Time
	var latestVersionID string
	for _, version := range z.Versions {
		if !version.Valid() {
			return nil, latestModTime, errFileCorrupt
		}
		var fi FileInfo
		switch version.Type {
		case ObjectType:
			fi, err = version.ObjectV2.ToFileInfo(volume, path)
		case DeleteType:
			fi, err = version.DeleteMarker.ToFileInfo(volume, path)
		case LegacyType:
			fi, err = version.ObjectV1.ToFileInfo(volume, path)
		}
		if err != nil {
			return nil, latestModTime, err
		}
		if fi.ModTime.After(latestModTime) {
			latestModTime = fi.ModTime
			latestVersionID = fi.VersionID
		}
		versions = append(versions, fi)
	}

	// We didn't find the version in delete markers so latest version
	// is indeed one of the actual version of the object with data.
	for i := range versions {
		if versions[i].VersionID != latestVersionID {
			continue
		}
		versions[i].IsLatest = true
		break
	}

	sort.Sort(versionsSorter(versions))
	return versions, latestModTime, nil
}

// ToFileInfo converts xlMetaV2 into a common FileInfo datastructure
// for consumption across callers.
func (z xlMetaV2) ToFileInfo(volume, path, versionID string) (fi FileInfo, err error) {
	var uv uuid.UUID
	if versionID != "" {
		uv, _ = uuid.Parse(versionID)
	}

	var latestModTime time.Time
	var latestIndex int
	for i, version := range z.Versions {
		if !version.Valid() {
			logger.LogIf(GlobalContext, fmt.Errorf("invalid version detected %#v", version))
			return FileInfo{}, errFileNotFound
		}
		var modTime time.Time
		switch version.Type {
		case ObjectType:
			modTime = time.Unix(0, version.ObjectV2.ModTime)
		case DeleteType:
			modTime = time.Unix(0, version.DeleteMarker.ModTime)
		case LegacyType:
			modTime = version.ObjectV1.Stat.ModTime
		}
		if modTime.After(latestModTime) {
			latestModTime = modTime
			latestIndex = i
		}
	}

	if versionID == "" {
		if len(z.Versions) >= 1 {
			if !z.Versions[latestIndex].Valid() {
				logger.LogIf(GlobalContext, fmt.Errorf("invalid version detected %#v", z.Versions[latestIndex]))
				return FileInfo{}, errFileNotFound
			}
			switch z.Versions[latestIndex].Type {
			case ObjectType:
				fi, err = z.Versions[latestIndex].ObjectV2.ToFileInfo(volume, path)
				fi.IsLatest = true
				return fi, err
			case DeleteType:
				fi, err = z.Versions[latestIndex].DeleteMarker.ToFileInfo(volume, path)
				fi.IsLatest = true
				return fi, err
			case LegacyType:
				fi, err = z.Versions[latestIndex].ObjectV1.ToFileInfo(volume, path)
				fi.IsLatest = true
				return fi, err
			}
		}
		return FileInfo{}, errFileNotFound
	}

	for _, version := range z.Versions {
		if !version.Valid() {
			logger.LogIf(GlobalContext, fmt.Errorf("invalid version detected %#v", version))
			if versionID == "" {
				return FileInfo{}, errFileNotFound
			}
			return FileInfo{}, errFileVersionNotFound
		}
		switch version.Type {
		case ObjectType:
			if bytes.Equal(version.ObjectV2.VersionID[:], uv[:]) {
				fi, err = version.ObjectV2.ToFileInfo(volume, path)
				fi.IsLatest = latestModTime.Equal(fi.ModTime)
				return fi, err
			}
		case LegacyType:
			if version.ObjectV1.VersionID == versionID {
				fi, err = version.ObjectV1.ToFileInfo(volume, path)
				fi.IsLatest = latestModTime.Equal(fi.ModTime)
				return fi, err
			}
		case DeleteType:
			if bytes.Equal(version.DeleteMarker.VersionID[:], uv[:]) {
				fi, err = version.DeleteMarker.ToFileInfo(volume, path)
				fi.IsLatest = latestModTime.Equal(fi.ModTime)
				return fi, err
			}
		}
	}

	if versionID == "" {
		return FileInfo{}, errFileNotFound
	}

	return FileInfo{}, errFileVersionNotFound
}
