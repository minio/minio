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
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/bpool"
	"github.com/minio/minio/internal/bucket/lifecycle"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/storageclass"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/tinylib/msgp/msgp"
)

var (
	// XL header specifies the format
	xlHeader = [4]byte{'X', 'L', '2', ' '}

	// Current version being written.
	xlVersionCurrent [4]byte
)

//msgp:clearomitted

//go:generate msgp -file=$GOFILE -unexported
//go:generate stringer -type VersionType,ErasureAlgo -output=xl-storage-format-v2_string.go $GOFILE

const (
	// Breaking changes.
	// Newer versions cannot be read by older software.
	// This will prevent downgrades to incompatible versions.
	xlVersionMajor = 1

	// Non breaking changes.
	// Bumping this is informational, but should be done
	// if any change is made to the data stored, bumping this
	// will allow to detect the exact version later.
	xlVersionMinor = 3
)

func init() {
	binary.LittleEndian.PutUint16(xlVersionCurrent[0:2], xlVersionMajor)
	binary.LittleEndian.PutUint16(xlVersionCurrent[2:4], xlVersionMinor)
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

// In addition to these we have a special kind called free-version. This is represented
// using a delete-marker and MetaSys entries. It's used to track tiered content of a
// deleted/overwritten version. This version is visible _only_to the scanner routine, for subsequent deletion.
// This kind of tracking is necessary since a version's tiered content is deleted asynchronously.

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
	VersionID [16]byte          `json:"ID" msg:"ID"`                               // Version ID for delete marker
	ModTime   int64             `json:"MTime" msg:"MTime"`                         // Object delete marker modified time
	MetaSys   map[string][]byte `json:"MetaSys,omitempty" msg:"MetaSys,omitempty"` // Delete marker internal metadata
}

// xlMetaV2Object defines the data struct for object journal type
type xlMetaV2Object struct {
	VersionID          [16]byte          `json:"ID" msg:"ID"`                                    // Version ID
	DataDir            [16]byte          `json:"DDir" msg:"DDir"`                                // Data dir ID
	ErasureAlgorithm   ErasureAlgo       `json:"EcAlgo" msg:"EcAlgo"`                            // Erasure coding algorithm
	ErasureM           int               `json:"EcM" msg:"EcM"`                                  // Erasure data blocks
	ErasureN           int               `json:"EcN" msg:"EcN"`                                  // Erasure parity blocks
	ErasureBlockSize   int64             `json:"EcBSize" msg:"EcBSize"`                          // Erasure block size
	ErasureIndex       int               `json:"EcIndex" msg:"EcIndex"`                          // Erasure disk index
	ErasureDist        []uint8           `json:"EcDist" msg:"EcDist"`                            // Erasure distribution
	BitrotChecksumAlgo ChecksumAlgo      `json:"CSumAlgo" msg:"CSumAlgo"`                        // Bitrot checksum algo
	PartNumbers        []int             `json:"PartNums" msg:"PartNums"`                        // Part Numbers
	PartETags          []string          `json:"PartETags" msg:"PartETags,allownil"`             // Part ETags
	PartSizes          []int64           `json:"PartSizes" msg:"PartSizes"`                      // Part Sizes
	PartActualSizes    []int64           `json:"PartASizes,omitempty" msg:"PartASizes,allownil"` // Part ActualSizes (compression)
	PartIndices        [][]byte          `json:"PartIndices,omitempty" msg:"PartIdx,omitempty"`  // Part Indexes (compression)
	Size               int64             `json:"Size" msg:"Size"`                                // Object version size
	ModTime            int64             `json:"MTime" msg:"MTime"`                              // Object version modified time
	MetaSys            map[string][]byte `json:"MetaSys,omitempty" msg:"MetaSys,allownil"`       // Object version internal metadata
	MetaUser           map[string]string `json:"MetaUsr,omitempty" msg:"MetaUsr,allownil"`       // Object version metadata set by user
}

// xlMetaV2Version describes the journal entry, Type defines
// the current journal entry type other types might be nil based
// on what Type field carries, it is imperative for the caller
// to verify which journal type first before accessing rest of the fields.
type xlMetaV2Version struct {
	Type             VersionType           `json:"Type" msg:"Type"`
	ObjectV1         *xlMetaV1Object       `json:"V1Obj,omitempty" msg:"V1Obj,omitempty"`
	ObjectV2         *xlMetaV2Object       `json:"V2Obj,omitempty" msg:"V2Obj,omitempty"`
	DeleteMarker     *xlMetaV2DeleteMarker `json:"DelObj,omitempty" msg:"DelObj,omitempty"`
	WrittenByVersion uint64                `msg:"v"` // Tracks written by MinIO version
}

// xlFlags contains flags on the object.
// This can be extended up to 64 bits without breaking compatibility.
type xlFlags uint8

const (
	xlFlagFreeVersion xlFlags = 1 << iota
	xlFlagUsesDataDir
	xlFlagInlineData
)

func (x xlFlags) String() string {
	var s strings.Builder
	if x&xlFlagFreeVersion != 0 {
		s.WriteString("FreeVersion")
	}
	if x&xlFlagUsesDataDir != 0 {
		if s.Len() > 0 {
			s.WriteByte(',')
		}
		s.WriteString("UsesDD")
	}
	if x&xlFlagInlineData != 0 {
		if s.Len() > 0 {
			s.WriteByte(',')
		}
		s.WriteString("Inline")
	}
	return s.String()
}

// checkXL2V1 will check if the metadata has correct header and is a known major version.
// The remaining payload and versions are returned.
func checkXL2V1(buf []byte) (payload []byte, major, minor uint16, err error) {
	if len(buf) <= 8 {
		return payload, 0, 0, fmt.Errorf("xlMeta: no data")
	}

	if !bytes.Equal(buf[:4], xlHeader[:]) {
		return payload, 0, 0, fmt.Errorf("xlMeta: unknown XLv2 header, expected %v, got %v", xlHeader[:4], buf[:4])
	}

	if bytes.Equal(buf[4:8], []byte("1   ")) {
		// Set as 1,0.
		major, minor = 1, 0
	} else {
		major, minor = binary.LittleEndian.Uint16(buf[4:6]), binary.LittleEndian.Uint16(buf[6:8])
	}
	if major > xlVersionMajor {
		return buf[8:], major, minor, fmt.Errorf("xlMeta: unknown major version %d found", major)
	}

	return buf[8:], major, minor, nil
}

func isXL2V1Format(buf []byte) bool {
	_, _, _, err := checkXL2V1(buf)
	return err == nil
}

//msgp:tuple xlMetaV2VersionHeader
type xlMetaV2VersionHeader struct {
	VersionID [16]byte
	ModTime   int64
	Signature [4]byte
	Type      VersionType
	Flags     xlFlags
	EcN, EcM  uint8 // Note that these will be 0/0 for non-v2 objects and older xl.meta
}

func (x xlMetaV2VersionHeader) String() string {
	return fmt.Sprintf("Type: %s, VersionID: %s, Signature: %s, ModTime: %s, Flags: %s, N: %d, M: %d",
		x.Type.String(),
		hex.EncodeToString(x.VersionID[:]),
		hex.EncodeToString(x.Signature[:]),
		time.Unix(0, x.ModTime),
		x.Flags.String(),
		x.EcN, x.EcM,
	)
}

// matchesNotStrict returns whether x and o have both have non-zero version,
// their versions match and their type match.
// If they have zero version, modtime must match.
func (x xlMetaV2VersionHeader) matchesNotStrict(o xlMetaV2VersionHeader) (ok bool) {
	ok = x.VersionID == o.VersionID && x.Type == o.Type && x.matchesEC(o)
	if x.VersionID == [16]byte{} {
		ok = ok && o.ModTime == x.ModTime
	}
	return ok
}

func (x xlMetaV2VersionHeader) matchesEC(o xlMetaV2VersionHeader) bool {
	if x.hasEC() && o.hasEC() {
		return x.EcN == o.EcN && x.EcM == o.EcM
	} // if no EC header this is an older object
	return true
}

// hasEC will return true if the version has erasure coding information.
func (x xlMetaV2VersionHeader) hasEC() bool {
	return x.EcM > 0 && x.EcN > 0
}

// sortsBefore can be used as a tiebreaker for stable sorting/selecting.
// Returns false on ties.
func (x xlMetaV2VersionHeader) sortsBefore(o xlMetaV2VersionHeader) bool {
	if x == o {
		return false
	}
	// Prefer newest modtime.
	if x.ModTime != o.ModTime {
		return x.ModTime > o.ModTime
	}

	// The following doesn't make too much sense, but we want sort to be consistent nonetheless.
	// Prefer lower types
	if x.Type != o.Type {
		return x.Type < o.Type
	}
	// Consistent sort on signature
	if v := bytes.Compare(x.Signature[:], o.Signature[:]); v != 0 {
		return v > 0
	}
	// On ID mismatch
	if v := bytes.Compare(x.VersionID[:], o.VersionID[:]); v != 0 {
		return v > 0
	}
	// Flags
	if x.Flags != o.Flags {
		return x.Flags > o.Flags
	}
	return false
}

func (j xlMetaV2Version) getDataDir() string {
	if j.Valid() {
		switch j.Type {
		case LegacyType:
			return j.ObjectV1.DataDir
		case ObjectType:
			return uuid.UUID(j.ObjectV2.DataDir).String()
		}
	}
	return ""
}

// Valid xl meta xlMetaV2Version is valid
func (j xlMetaV2Version) Valid() bool {
	if !j.Type.valid() {
		return false
	}
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

// header will return a shallow header of the version.
func (j *xlMetaV2Version) header() xlMetaV2VersionHeader {
	var flags xlFlags
	if j.FreeVersion() {
		flags |= xlFlagFreeVersion
	}
	if j.Type == ObjectType && j.ObjectV2.UsesDataDir() {
		flags |= xlFlagUsesDataDir
	}
	if j.Type == ObjectType && j.ObjectV2.InlineData() {
		flags |= xlFlagInlineData
	}
	var ecM, ecN uint8
	if j.Type == ObjectType && j.ObjectV2 != nil {
		ecM, ecN = uint8(j.ObjectV2.ErasureM), uint8(j.ObjectV2.ErasureN)
	}
	return xlMetaV2VersionHeader{
		VersionID: j.getVersionID(),
		ModTime:   j.getModTime().UnixNano(),
		Signature: j.getSignature(),
		Type:      j.Type,
		Flags:     flags,
		EcN:       ecN,
		EcM:       ecM,
	}
}

// FreeVersion returns true if x represents a free-version, false otherwise.
func (x xlMetaV2VersionHeader) FreeVersion() bool {
	return x.Flags&xlFlagFreeVersion != 0
}

// UsesDataDir returns true if this object version uses its data directory for
// its contents and false otherwise.
func (x xlMetaV2VersionHeader) UsesDataDir() bool {
	return x.Flags&xlFlagUsesDataDir != 0
}

// InlineData returns whether inline data has been set.
// Note that false does not mean there is no inline data,
// only that it is unlikely.
func (x xlMetaV2VersionHeader) InlineData() bool {
	return x.Flags&xlFlagInlineData != 0
}

// signatureErr is a signature returned when an error occurs.
var signatureErr = [4]byte{'e', 'r', 'r', 0}

// getSignature will return a signature that is expected to be the same across all disks.
func (j xlMetaV2Version) getSignature() [4]byte {
	switch j.Type {
	case ObjectType:
		return j.ObjectV2.Signature()
	case DeleteType:
		return j.DeleteMarker.Signature()
	case LegacyType:
		return j.ObjectV1.Signature()
	}
	return signatureErr
}

// getModTime will return the ModTime of the underlying version.
func (j xlMetaV2Version) getModTime() time.Time {
	switch j.Type {
	case ObjectType:
		return time.Unix(0, j.ObjectV2.ModTime)
	case DeleteType:
		return time.Unix(0, j.DeleteMarker.ModTime)
	case LegacyType:
		return j.ObjectV1.Stat.ModTime
	}
	return time.Time{}
}

// getVersionID will return the versionID of the underlying version.
func (j xlMetaV2Version) getVersionID() [16]byte {
	switch j.Type {
	case ObjectType:
		return j.ObjectV2.VersionID
	case DeleteType:
		return j.DeleteMarker.VersionID
	case LegacyType:
		return [16]byte{}
	}
	return [16]byte{}
}

// ToFileInfo returns FileInfo of the underlying type.
func (j *xlMetaV2Version) ToFileInfo(volume, path string, allParts bool) (fi FileInfo, err error) {
	if j == nil {
		return fi, errFileNotFound
	}
	switch j.Type {
	case ObjectType:
		fi, err = j.ObjectV2.ToFileInfo(volume, path, allParts)
	case DeleteType:
		fi, err = j.DeleteMarker.ToFileInfo(volume, path)
	case LegacyType:
		fi, err = j.ObjectV1.ToFileInfo(volume, path)
	default:
		return fi, errFileNotFound
	}
	fi.WrittenByVersion = j.WrittenByVersion
	return fi, err
}

const (
	xlHeaderVersion = 3
	xlMetaVersion   = 3
)

func (j xlMetaV2DeleteMarker) ToFileInfo(volume, path string) (FileInfo, error) {
	versionID := ""
	var uv uuid.UUID
	// check if the version is not "null"
	if j.VersionID != uv {
		versionID = uuid.UUID(j.VersionID).String()
	}
	fi := FileInfo{
		Volume:    volume,
		Name:      path,
		ModTime:   time.Unix(0, j.ModTime).UTC(),
		VersionID: versionID,
		Deleted:   true,
	}
	fi.Metadata = make(map[string]string, len(j.MetaSys))
	for k, v := range j.MetaSys {
		fi.Metadata[k] = string(v)
	}

	fi.ReplicationState = GetInternalReplicationState(j.MetaSys)
	if j.FreeVersion() {
		fi.SetTierFreeVersion()
		fi.TransitionTier = string(j.MetaSys[metaTierName])
		fi.TransitionedObjName = string(j.MetaSys[metaTierObjName])
		fi.TransitionVersionID = string(j.MetaSys[metaTierVersionID])
	}

	return fi, nil
}

// Signature will return a signature that is expected to be the same across all disks.
func (j *xlMetaV2DeleteMarker) Signature() [4]byte {
	// Shallow copy
	c := *j

	// Marshal metadata
	crc := hashDeterministicBytes(c.MetaSys)
	c.MetaSys = nil
	if bts, err := c.MarshalMsg(metaDataPoolGet()); err == nil {
		crc ^= xxhash.Sum64(bts)
		metaDataPoolPut(bts)
	}

	// Combine upper and lower part
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(crc^(crc>>32)))
	return tmp
}

// UsesDataDir returns true if this object version uses its data directory for
// its contents and false otherwise.
func (j xlMetaV2Object) UsesDataDir() bool {
	// Skip if this version is not transitioned, i.e it uses its data directory.
	if !bytes.Equal(j.MetaSys[metaTierStatus], []byte(lifecycle.TransitionComplete)) {
		return true
	}

	// Check if this transitioned object has been restored on disk.
	return isRestoredObjectOnDisk(j.MetaUser)
}

// InlineData returns whether inline data has been set.
// Note that false does not mean there is no inline data,
// only that it is unlikely.
func (j xlMetaV2Object) InlineData() bool {
	_, ok := j.MetaSys[ReservedMetadataPrefixLower+"inline-data"]
	return ok
}

func (j *xlMetaV2Object) ResetInlineData() {
	delete(j.MetaSys, ReservedMetadataPrefixLower+"inline-data")
}

const (
	metaTierStatus    = ReservedMetadataPrefixLower + TransitionStatus
	metaTierObjName   = ReservedMetadataPrefixLower + TransitionedObjectName
	metaTierVersionID = ReservedMetadataPrefixLower + TransitionedVersionID
	metaTierName      = ReservedMetadataPrefixLower + TransitionTier
)

func (j *xlMetaV2Object) SetTransition(fi FileInfo) {
	j.MetaSys[metaTierStatus] = []byte(fi.TransitionStatus)
	j.MetaSys[metaTierObjName] = []byte(fi.TransitionedObjName)
	j.MetaSys[metaTierVersionID] = []byte(fi.TransitionVersionID)
	j.MetaSys[metaTierName] = []byte(fi.TransitionTier)
}

func (j *xlMetaV2Object) RemoveRestoreHdrs() {
	delete(j.MetaUser, xhttp.AmzRestore)
	delete(j.MetaUser, xhttp.AmzRestoreExpiryDays)
	delete(j.MetaUser, xhttp.AmzRestoreRequestDate)
}

// Signature will return a signature that is expected to be the same across all disks.
func (j *xlMetaV2Object) Signature() [4]byte {
	// Shallow copy
	c := *j
	// Zero fields that will vary across disks
	c.ErasureIndex = 0

	// Nil 0 size allownil, so we don't differentiate between nil and 0 len.
	allEmpty := true
	for _, tag := range c.PartETags {
		if len(tag) != 0 {
			allEmpty = false
			break
		}
	}
	if allEmpty {
		c.PartETags = nil
	}
	if len(c.PartActualSizes) == 0 {
		c.PartActualSizes = nil
	}

	// Get a 64 bit CRC
	crc := hashDeterministicString(c.MetaUser)
	crc ^= hashDeterministicBytes(c.MetaSys)

	// Nil fields.
	c.MetaSys = nil
	c.MetaUser = nil

	if bts, err := c.MarshalMsg(metaDataPoolGet()); err == nil {
		crc ^= xxhash.Sum64(bts)
		metaDataPoolPut(bts)
	}

	// Combine upper and lower part
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(crc^(crc>>32)))
	return tmp
}

func (j xlMetaV2Object) ToFileInfo(volume, path string, allParts bool) (FileInfo, error) {
	versionID := ""
	var uv uuid.UUID
	// check if the version is not "null"
	if j.VersionID != uv {
		versionID = uuid.UUID(j.VersionID).String()
	}
	fi := FileInfo{
		Volume:    volume,
		Name:      path,
		Size:      j.Size,
		ModTime:   time.Unix(0, j.ModTime).UTC(),
		VersionID: versionID,
	}
	if allParts {
		fi.Parts = make([]ObjectPartInfo, len(j.PartNumbers))
		for i := range fi.Parts {
			fi.Parts[i].Number = j.PartNumbers[i]
			fi.Parts[i].Size = j.PartSizes[i]
			if len(j.PartETags) == len(fi.Parts) {
				fi.Parts[i].ETag = j.PartETags[i]
			}
			fi.Parts[i].ActualSize = j.PartActualSizes[i]
			if len(j.PartIndices) == len(fi.Parts) {
				fi.Parts[i].Index = j.PartIndices[i]
			}
		}
	}

	// fi.Erasure.Checksums - is left empty since we do not have any
	// whole checksums for many years now, no need to allocate.

	fi.Metadata = make(map[string]string, len(j.MetaUser)+len(j.MetaSys))
	for k, v := range j.MetaUser {
		// https://github.com/google/security-research/security/advisories/GHSA-76wf-9vgp-pj7w
		if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
			continue
		}
		if equals(k, "x-amz-storage-class") && v == storageclass.STANDARD {
			continue
		}

		fi.Metadata[k] = v
	}

	tierFVIDKey := ReservedMetadataPrefixLower + tierFVID
	tierFVMarkerKey := ReservedMetadataPrefixLower + tierFVMarker
	for k, v := range j.MetaSys {
		// Make sure we skip free-version-id, similar to AddVersion()
		if len(k) > len(ReservedMetadataPrefixLower) && strings.EqualFold(k[:len(ReservedMetadataPrefixLower)], ReservedMetadataPrefixLower) {
			// Skip tierFVID, tierFVMarker keys; it's used
			// only for creating free-version.
			switch k {
			case tierFVIDKey, tierFVMarkerKey:
				continue
			}
		}
		if equals(k, "x-amz-storage-class") && string(v) == storageclass.STANDARD {
			continue
		}
		switch {
		case strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower), equals(k, VersionPurgeStatusKey):
			fi.Metadata[k] = string(v)
		}
	}
	fi.ReplicationState = getInternalReplicationState(fi.Metadata)
	fi.Deleted = !fi.VersionPurgeStatus().Empty()
	replStatus := fi.ReplicationState.CompositeReplicationStatus()
	if replStatus != "" {
		fi.Metadata[xhttp.AmzBucketReplicationStatus] = string(replStatus)
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

	if st, ok := j.MetaSys[metaTierStatus]; ok {
		fi.TransitionStatus = string(st)
	}
	if o, ok := j.MetaSys[metaTierObjName]; ok {
		fi.TransitionedObjName = string(o)
	}
	if rv, ok := j.MetaSys[metaTierVersionID]; ok {
		fi.TransitionVersionID = string(rv)
	}
	if sc, ok := j.MetaSys[metaTierName]; ok {
		fi.TransitionTier = string(sc)
	}
	if crcs := j.MetaSys[ReservedMetadataPrefixLower+"crc"]; len(crcs) > 0 {
		fi.Checksum = crcs
	}
	return fi, nil
}

// Read at most this much on initial read.
const metaDataReadDefault = 4 << 10

// Return used metadata byte slices here.
var metaDataPool = bpool.Pool[[]byte]{New: func() []byte { return make([]byte, 0, metaDataReadDefault) }}

// metaDataPoolGet will return a byte slice with capacity at least metaDataReadDefault.
// It will be length 0.
func metaDataPoolGet() []byte {
	return metaDataPool.Get()[:0]
}

// metaDataPoolPut will put an unused small buffer back into the pool.
func metaDataPoolPut(buf []byte) {
	if cap(buf) >= metaDataReadDefault && cap(buf) < metaDataReadDefault*4 {
		metaDataPool.Put(buf)
	}
}

// readXLMetaNoData will load the metadata, but skip data segments.
// This should only be used when data is never interesting.
// If data is not xlv2, it is returned in full.
func readXLMetaNoData(r io.Reader, size int64) ([]byte, error) {
	initial := size
	hasFull := true
	if initial > metaDataReadDefault {
		initial = metaDataReadDefault
		hasFull = false
	}

	buf := metaDataPoolGet()[:initial]
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("readXLMetaNoData(io.ReadFull): %w", err)
	}
	readMore := func(n int64) error {
		has := int64(len(buf))
		if has >= n {
			return nil
		}
		if hasFull || n > size {
			return io.ErrUnexpectedEOF
		}
		extra := n - has
		if int64(cap(buf)) >= n {
			// Extend since we have enough space.
			buf = buf[:n]
		} else {
			buf = append(buf, make([]byte, extra)...)
		}
		_, err := io.ReadFull(r, buf[has:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Returned if we read nothing.
				err = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("readXLMetaNoData(readMore): %w", err)
		}
		return nil
	}
	tmp, major, minor, err := checkXL2V1(buf)
	if err != nil {
		err = readMore(size)
		return buf, err
	}
	switch major {
	case 1:
		switch minor {
		case 0:
			err = readMore(size)
			return buf, err
		case 1, 2, 3:
			sz, tmp, err := msgp.ReadBytesHeader(tmp)
			if err != nil {
				return nil, fmt.Errorf("readXLMetaNoData(read_meta): unknown metadata version %w", err)
			}
			want := int64(sz) + int64(len(buf)-len(tmp))

			// v1.1 does not have CRC.
			if minor < 2 {
				if err := readMore(want); err != nil {
					return nil, err
				}
				return buf[:want], nil
			}

			// CRC is variable length, so we need to truncate exactly that.
			wantMax := min(want+msgp.Uint32Size, size)
			if err := readMore(wantMax); err != nil {
				return nil, err
			}

			if int64(len(buf)) < want {
				return nil, fmt.Errorf("buffer shorter than expected (buflen: %d, want: %d): %w", len(buf), want, errFileCorrupt)
			}

			tmp = buf[want:]
			_, after, err := msgp.ReadUint32Bytes(tmp)
			if err != nil {
				return nil, fmt.Errorf("readXLMetaNoData(read_meta): unknown metadata version %w", err)
			}
			want += int64(len(tmp) - len(after))

			return buf[:want], err

		default:
			return nil, errors.New("unknown minor metadata version")
		}
	default:
		return nil, errors.New("unknown major metadata version")
	}
}

func decodeXLHeaders(buf []byte) (versions int, headerV, metaV uint8, b []byte, err error) {
	hdrVer, buf, err := msgp.ReadUint8Bytes(buf)
	if err != nil {
		return 0, 0, 0, buf, err
	}
	metaVer, buf, err := msgp.ReadUint8Bytes(buf)
	if err != nil {
		return 0, 0, 0, buf, err
	}
	if hdrVer > xlHeaderVersion {
		return 0, 0, 0, buf, fmt.Errorf("decodeXLHeaders: Unknown xl header version %d", hdrVer)
	}
	if metaVer > xlMetaVersion {
		return 0, 0, 0, buf, fmt.Errorf("decodeXLHeaders: Unknown xl meta version %d", metaVer)
	}
	versions, buf, err = msgp.ReadIntBytes(buf)
	if err != nil {
		return 0, 0, 0, buf, err
	}
	if versions < 0 {
		return 0, 0, 0, buf, fmt.Errorf("decodeXLHeaders: Negative version count %d", versions)
	}
	return versions, hdrVer, metaVer, buf, nil
}

// decodeVersions will decode a number of versions from a buffer
// and perform a callback for each version in order, newest first.
// Return errDoneForNow to stop processing and return nil.
// Any non-nil error is returned.
func decodeVersions(buf []byte, versions int, fn func(idx int, hdr, meta []byte) error) (err error) {
	var tHdr, tMeta []byte // Zero copy bytes
	for i := range versions {
		tHdr, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return err
		}
		tMeta, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return err
		}
		if err = fn(i, tHdr, tMeta); err != nil {
			if err == errDoneForNow {
				err = nil
			}
			return err
		}
	}
	return nil
}

// isIndexedMetaV2 returns non-nil result if metadata is indexed.
// Returns 3x nil if not XLV2 or not indexed.
// If indexed and unable to parse an error will be returned.
func isIndexedMetaV2(buf []byte) (meta xlMetaBuf, data xlMetaInlineData, err error) {
	buf, major, minor, err := checkXL2V1(buf)
	if err != nil || major != 1 || minor < 3 {
		return nil, nil, nil
	}
	meta, buf, err = msgp.ReadBytesZC(buf)
	if err != nil {
		return nil, nil, err
	}
	if crc, nbuf, err := msgp.ReadUint32Bytes(buf); err == nil {
		// Read metadata CRC
		buf = nbuf
		if got := uint32(xxhash.Sum64(meta)); got != crc {
			return nil, nil, fmt.Errorf("xlMetaV2.Load version(%d), CRC mismatch, want 0x%x, got 0x%x", minor, crc, got)
		}
	} else {
		return nil, nil, err
	}
	data = buf
	if data.validate() != nil {
		data.repair()
	}

	return meta, data, nil
}

type xlMetaV2ShallowVersion struct {
	header xlMetaV2VersionHeader
	meta   []byte
}

//msgp:ignore xlMetaV2 xlMetaV2ShallowVersion

type xlMetaV2 struct {
	versions []xlMetaV2ShallowVersion

	// data will contain raw data if any.
	// data will be one or more versions indexed by versionID.
	// To remove all data set to nil.
	data xlMetaInlineData

	// metadata version.
	metaV uint8
}

// LoadOrConvert will load the metadata in the buffer.
// If this is a legacy format, it will automatically be converted to XLV2.
func (x *xlMetaV2) LoadOrConvert(buf []byte) error {
	if isXL2V1Format(buf) {
		return x.Load(buf)
	}

	xlMeta := &xlMetaV1Object{}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(buf, xlMeta); err != nil {
		return errFileCorrupt
	}
	if len(x.versions) > 0 {
		x.versions = x.versions[:0]
	}
	x.data = nil
	x.metaV = xlMetaVersion
	return x.AddLegacy(xlMeta)
}

// Load all versions of the stored data.
// Note that references to the incoming buffer will be kept.
func (x *xlMetaV2) Load(buf []byte) error {
	if meta, data, err := isIndexedMetaV2(buf); err != nil {
		return err
	} else if meta != nil {
		return x.loadIndexed(meta, data)
	}
	// Convert older format.
	return x.loadLegacy(buf)
}

func (x *xlMetaV2) loadIndexed(buf xlMetaBuf, data xlMetaInlineData) error {
	versions, headerV, metaV, buf, err := decodeXLHeaders(buf)
	if err != nil {
		return err
	}
	if cap(x.versions) < versions {
		x.versions = make([]xlMetaV2ShallowVersion, 0, versions+1)
	}
	x.versions = x.versions[:versions]
	x.data = data
	x.metaV = metaV
	if err = x.data.validate(); err != nil {
		x.data.repair()
		storageLogIf(GlobalContext, fmt.Errorf("xlMetaV2.loadIndexed: data validation failed: %v. %d entries after repair", err, x.data.entries()))
	}
	return decodeVersions(buf, versions, func(i int, hdr, meta []byte) error {
		ver := &x.versions[i]
		_, err = ver.header.unmarshalV(headerV, hdr)
		if err != nil {
			return err
		}
		ver.meta = meta

		// Fix inconsistent compression index due to https://github.com/minio/minio/pull/20575
		// First search marshaled content for encoded values.
		// We have bumped metaV to make this check cheaper.
		if metaV < 3 && ver.header.Type == ObjectType && bytes.Contains(meta, []byte("\xa7PartIdx")) &&
			bytes.Contains(meta, []byte("\xbcX-Minio-Internal-compression\xc4\x15klauspost/compress/s2")) {
			// Likely candidate...
			version, err := x.getIdx(i)
			if err == nil {
				// Check write date...
				// RELEASE.2023-12-02T10-51-33Z -> RELEASE.2024-10-29T16-01-48Z
				const dateStart = 1701471618
				const dateEnd = 1730156418
				if version.WrittenByVersion > dateStart && version.WrittenByVersion < dateEnd &&
					version.ObjectV2 != nil && len(version.ObjectV2.PartIndices) > 0 {
					var changed bool
					clearField := true
					for i, sz := range version.ObjectV2.PartActualSizes {
						if len(version.ObjectV2.PartIndices) > i {
							// 8<<20 is current 'compMinIndexSize', but we detach it in case it should change in the future.
							if sz <= 8<<20 && len(version.ObjectV2.PartIndices[i]) > 0 {
								changed = true
								version.ObjectV2.PartIndices[i] = nil
							}
							clearField = clearField && len(version.ObjectV2.PartIndices[i]) == 0
						}
					}
					if changed {
						// All empty, clear.
						if clearField {
							version.ObjectV2.PartIndices = nil
						}

						// Reindex since it was changed.
						meta, err := version.MarshalMsg(make([]byte, 0, len(ver.meta)+10))
						if err == nil {
							// Override both if fine.
							ver.header = version.header()
							ver.meta = meta
						}
					}
				}
			}
		}

		// Fix inconsistent x-minio-internal-replication-timestamp by loading and reindexing.
		if metaV < 2 && ver.header.Type == DeleteType {
			// load (and convert) version.
			version, err := x.getIdx(i)
			if err == nil {
				// Only reindex if set.
				_, ok1 := version.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicationTimestamp]
				_, ok2 := version.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicaTimestamp]
				if ok1 || ok2 {
					meta, err := version.MarshalMsg(make([]byte, 0, len(ver.meta)+10))
					if err == nil {
						// Override both if fine.
						ver.header = version.header()
						ver.meta = meta
					}
				}
			}
		}
		return nil
	})
}

// loadLegacy will load content prior to v1.3
// Note that references to the incoming buffer will be kept.
func (x *xlMetaV2) loadLegacy(buf []byte) error {
	buf, major, minor, err := checkXL2V1(buf)
	if err != nil {
		return fmt.Errorf("xlMetaV2.Load %w", err)
	}
	var allMeta []byte
	switch major {
	case 1:
		switch minor {
		case 0:
			allMeta = buf
		case 1, 2:
			v, buf, err := msgp.ReadBytesZC(buf)
			if err != nil {
				return fmt.Errorf("xlMetaV2.Load version(%d), bufLen(%d) %w", minor, len(buf), err)
			}
			if minor >= 2 {
				if crc, nbuf, err := msgp.ReadUint32Bytes(buf); err == nil {
					// Read metadata CRC (added in v2)
					buf = nbuf
					if got := uint32(xxhash.Sum64(v)); got != crc {
						return fmt.Errorf("xlMetaV2.Load version(%d), CRC mismatch, want 0x%x, got 0x%x", minor, crc, got)
					}
				} else {
					return fmt.Errorf("xlMetaV2.Load version(%d), loading CRC: %w", minor, err)
				}
			}

			allMeta = v
			// Add remaining data.
			x.data = buf
			if err = x.data.validate(); err != nil {
				x.data.repair()
				storageLogIf(GlobalContext, fmt.Errorf("xlMetaV2.Load: data validation failed: %v. %d entries after repair", err, x.data.entries()))
			}
		default:
			return errors.New("unknown minor metadata version")
		}
	default:
		return errors.New("unknown major metadata version")
	}
	if allMeta == nil {
		return errFileCorrupt
	}
	// bts will shrink as we decode.
	bts := allMeta
	var field []byte
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return msgp.WrapError(err, "loadLegacy.ReadMapHeader")
	}

	var tmp xlMetaV2Version
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return msgp.WrapError(err, "loadLegacy.ReadMapKey")
		}
		switch msgp.UnsafeString(field) {
		case "Versions":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return msgp.WrapError(err, "Versions")
			}
			if cap(x.versions) >= int(zb0002) {
				x.versions = (x.versions)[:zb0002]
			} else {
				x.versions = make([]xlMetaV2ShallowVersion, zb0002, zb0002+1)
			}
			for za0001 := range x.versions {
				start := len(allMeta) - len(bts)
				bts, err = tmp.unmarshalV(1, bts)
				if err != nil {
					return msgp.WrapError(err, "Versions", za0001)
				}
				end := len(allMeta) - len(bts)
				// We reference the marshaled data, so we don't have to re-marshal.
				x.versions[za0001] = xlMetaV2ShallowVersion{
					header: tmp.header(),
					meta:   allMeta[start:end],
				}
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return msgp.WrapError(err, "loadLegacy.Skip")
			}
		}
	}
	x.metaV = 1 // Fixed for legacy conversions.
	x.sortByModTime()
	return nil
}

// latestModtime returns the modtime of the latest version.
func (x *xlMetaV2) latestModtime() time.Time {
	if x == nil || len(x.versions) == 0 {
		return time.Time{}
	}
	return time.Unix(0, x.versions[0].header.ModTime)
}

func (x *xlMetaV2) addVersion(ver xlMetaV2Version) error {
	modTime := ver.getModTime().UnixNano()
	if !ver.Valid() {
		return errors.New("attempted to add invalid version")
	}
	encoded, err := ver.MarshalMsg(nil)
	if err != nil {
		return err
	}

	// returns error if we have exceeded configured object max versions
	if int64(len(x.versions)+1) > globalAPIConfig.getObjectMaxVersions() {
		return errMaxVersionsExceeded
	}

	// Add space at the end.
	// Will have -1 modtime, so it will be inserted there.
	x.versions = append(x.versions, xlMetaV2ShallowVersion{header: xlMetaV2VersionHeader{ModTime: -1}})

	// Linear search, we likely have to insert at front.
	for i, existing := range x.versions {
		if existing.header.ModTime <= modTime {
			// Insert at current idx. First move current back.
			copy(x.versions[i+1:], x.versions[i:])
			x.versions[i] = xlMetaV2ShallowVersion{
				header: ver.header(),
				meta:   encoded,
			}
			return nil
		}
	}
	return fmt.Errorf("addVersion: Internal error, unable to add version")
}

// AppendTo will marshal the data in z and append it to the provided slice.
func (x *xlMetaV2) AppendTo(dst []byte) ([]byte, error) {
	// Header...
	sz := len(xlHeader) + len(xlVersionCurrent) + msgp.ArrayHeaderSize + len(dst) + 3*msgp.Uint32Size
	// Existing + Inline data
	sz += len(dst) + len(x.data)
	// Versions...
	for _, ver := range x.versions {
		sz += 32 + len(ver.meta)
	}
	if cap(dst) < sz {
		buf := make([]byte, len(dst), sz)
		copy(buf, dst)
		dst = buf
	}
	if err := x.data.validate(); err != nil {
		return nil, err
	}

	dst = append(dst, xlHeader[:]...)
	dst = append(dst, xlVersionCurrent[:]...)
	// Add "bin 32" type header to always have enough space.
	// We will fill out the correct size when we know it.
	dst = append(dst, 0xc6, 0, 0, 0, 0)
	dataOffset := len(dst)

	dst = msgp.AppendUint(dst, xlHeaderVersion)
	dst = msgp.AppendUint(dst, xlMetaVersion)
	dst = msgp.AppendInt(dst, len(x.versions))

	tmp := metaDataPoolGet()
	defer metaDataPoolPut(tmp)
	for _, ver := range x.versions {
		var err error

		// Add header
		tmp, err = ver.header.MarshalMsg(tmp[:0])
		if err != nil {
			return nil, err
		}
		dst = msgp.AppendBytes(dst, tmp)

		// Add full meta
		dst = msgp.AppendBytes(dst, ver.meta)
	}

	// Update size...
	binary.BigEndian.PutUint32(dst[dataOffset-4:dataOffset], uint32(len(dst)-dataOffset))

	// Add CRC of metadata as fixed size (5 bytes)
	// Prior to v1.3 this was variable sized.
	tmp = tmp[:5]
	tmp[0] = 0xce // muint32
	binary.BigEndian.PutUint32(tmp[1:], uint32(xxhash.Sum64(dst[dataOffset:])))
	dst = append(dst, tmp[:5]...)
	return append(dst, x.data...), nil
}

const emptyUUID = "00000000-0000-0000-0000-000000000000"

func (x *xlMetaV2) findVersionStr(key string) (idx int, ver *xlMetaV2Version, err error) {
	if key == nullVersionID {
		key = ""
	}
	var u uuid.UUID
	if key != "" {
		u, err = uuid.Parse(key)
		if err != nil {
			return -1, nil, errFileVersionNotFound
		}
	}
	return x.findVersion(u)
}

func (x *xlMetaV2) findVersion(key [16]byte) (idx int, ver *xlMetaV2Version, err error) {
	for i, ver := range x.versions {
		if key == ver.header.VersionID {
			obj, err := x.getIdx(i)
			return i, obj, err
		}
	}
	return -1, nil, errFileVersionNotFound
}

func (x *xlMetaV2) getIdx(idx int) (ver *xlMetaV2Version, err error) {
	if idx < 0 || idx >= len(x.versions) {
		return nil, errFileNotFound
	}
	var dst xlMetaV2Version
	_, err = dst.unmarshalV(x.metaV, x.versions[idx].meta)
	if false {
		if err == nil && x.versions[idx].header.VersionID != dst.getVersionID() {
			panic(fmt.Sprintf("header: %x != object id: %x", x.versions[idx].header.VersionID, dst.getVersionID()))
		}
	}
	return &dst, err
}

// setIdx will replace a version at a given index.
// Note that versions may become re-sorted if modtime changes.
func (x *xlMetaV2) setIdx(idx int, ver xlMetaV2Version) (err error) {
	if idx < 0 || idx >= len(x.versions) {
		return errFileNotFound
	}
	update := &x.versions[idx]
	prevMod := update.header.ModTime
	update.meta, err = ver.MarshalMsg(update.meta[:0:len(update.meta)])
	if err != nil {
		update.meta = nil
		return err
	}
	update.header = ver.header()
	if prevMod != update.header.ModTime {
		x.sortByModTime()
	}
	return nil
}

// getDataDirs will return all data directories in the metadata
// as well as all version ids used for inline data.
func (x *xlMetaV2) getDataDirs() ([]string, error) {
	dds := make([]string, 0, len(x.versions)*2)
	for i, ver := range x.versions {
		if ver.header.Type == DeleteType {
			continue
		}

		obj, err := x.getIdx(i)
		if err != nil {
			return nil, err
		}
		switch ver.header.Type {
		case ObjectType:
			if obj.ObjectV2 == nil {
				return nil, errors.New("obj.ObjectV2 unexpectedly nil")
			}
			dds = append(dds, uuid.UUID(obj.ObjectV2.DataDir).String())
			if obj.ObjectV2.VersionID == [16]byte{} {
				dds = append(dds, nullVersionID)
			} else {
				dds = append(dds, uuid.UUID(obj.ObjectV2.VersionID).String())
			}
		case LegacyType:
			if obj.ObjectV1 == nil {
				return nil, errors.New("obj.ObjectV1 unexpectedly nil")
			}
			dds = append(dds, obj.ObjectV1.DataDir)
		}
	}
	return dds, nil
}

// sortByModTime will sort versions by modtime in descending order,
// meaning index 0 will be latest version.
func (x *xlMetaV2) sortByModTime() {
	// Quick check
	if len(x.versions) <= 1 || sort.SliceIsSorted(x.versions, func(i, j int) bool {
		return x.versions[i].header.sortsBefore(x.versions[j].header)
	}) {
		return
	}

	// We should sort.
	sort.Slice(x.versions, func(i, j int) bool {
		return x.versions[i].header.sortsBefore(x.versions[j].header)
	})
}

// DeleteVersion deletes the version specified by version id.
// returns to the caller which dataDir to delete, also
// indicates if this is the last version.
func (x *xlMetaV2) DeleteVersion(fi FileInfo) (string, error) {
	// This is a situation where versionId is explicitly
	// specified as "null", as we do not save "null"
	// string it is considered empty. But empty also
	// means the version which matches will be purged.
	if fi.VersionID == nullVersionID {
		fi.VersionID = ""
	}

	var uv uuid.UUID
	var err error
	if fi.VersionID != "" {
		uv, err = uuid.Parse(fi.VersionID)
		if err != nil {
			return "", errFileVersionNotFound
		}
	}

	var ventry xlMetaV2Version
	if fi.Deleted {
		ventry = xlMetaV2Version{
			Type: DeleteType,
			DeleteMarker: &xlMetaV2DeleteMarker{
				VersionID: uv,
				ModTime:   fi.ModTime.UnixNano(),
				MetaSys:   make(map[string][]byte),
			},
			WrittenByVersion: globalVersionUnix,
		}
		if !ventry.Valid() {
			return "", errors.New("internal error: invalid version entry generated")
		}
	}
	updateVersion := false
	if fi.VersionPurgeStatus().Empty() && (fi.DeleteMarkerReplicationStatus() == "REPLICA" || fi.DeleteMarkerReplicationStatus().Empty()) {
		updateVersion = fi.MarkDeleted
	} else {
		// for replication scenario
		if fi.Deleted && fi.VersionPurgeStatus() != replication.VersionPurgeComplete {
			if !fi.VersionPurgeStatus().Empty() || fi.DeleteMarkerReplicationStatus().Empty() {
				updateVersion = true
			}
		}
		// object or delete-marker versioned delete is not complete
		if !fi.VersionPurgeStatus().Empty() && fi.VersionPurgeStatus() != replication.VersionPurgeComplete {
			updateVersion = true
		}
	}

	if fi.Deleted {
		if !fi.DeleteMarkerReplicationStatus().Empty() {
			switch fi.DeleteMarkerReplicationStatus() {
			case replication.Replica:
				ventry.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicaStatus] = []byte(fi.ReplicationState.ReplicaStatus)
				ventry.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicaTimestamp] = []byte(fi.ReplicationState.ReplicaTimeStamp.UTC().Format(time.RFC3339Nano))
			default:
				ventry.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicationStatus] = []byte(fi.ReplicationState.ReplicationStatusInternal)
				ventry.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicationTimestamp] = []byte(fi.ReplicationState.ReplicationTimeStamp.UTC().Format(time.RFC3339Nano))
			}
		}
		if !fi.VersionPurgeStatus().Empty() {
			ventry.DeleteMarker.MetaSys[VersionPurgeStatusKey] = []byte(fi.ReplicationState.VersionPurgeStatusInternal)
		}
		for k, v := range fi.ReplicationState.ResetStatusesMap {
			ventry.DeleteMarker.MetaSys[k] = []byte(v)
		}
	}

	for i, ver := range x.versions {
		if ver.header.VersionID != uv {
			continue
		}
		switch ver.header.Type {
		case LegacyType:
			ver, err := x.getIdx(i)
			if err != nil {
				return "", err
			}
			x.versions = append(x.versions[:i], x.versions[i+1:]...)
			if fi.Deleted {
				err = x.addVersion(ventry)
			}
			return ver.ObjectV1.DataDir, err
		case DeleteType:
			if updateVersion {
				ver, err := x.getIdx(i)
				if err != nil {
					return "", err
				}
				if len(ver.DeleteMarker.MetaSys) == 0 {
					ver.DeleteMarker.MetaSys = make(map[string][]byte)
				}
				if !fi.DeleteMarkerReplicationStatus().Empty() {
					switch fi.DeleteMarkerReplicationStatus() {
					case replication.Replica:
						ver.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicaStatus] = []byte(fi.ReplicationState.ReplicaStatus)
						ver.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicaTimestamp] = []byte(fi.ReplicationState.ReplicaTimeStamp.UTC().Format(time.RFC3339Nano))
					default:
						ver.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicationStatus] = []byte(fi.ReplicationState.ReplicationStatusInternal)
						ver.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+ReplicationTimestamp] = []byte(fi.ReplicationState.ReplicationTimeStamp.UTC().Format(time.RFC3339Nano))
					}
				}
				if !fi.VersionPurgeStatus().Empty() {
					ver.DeleteMarker.MetaSys[VersionPurgeStatusKey] = []byte(fi.ReplicationState.VersionPurgeStatusInternal)
				}
				for k, v := range fi.ReplicationState.ResetStatusesMap {
					ver.DeleteMarker.MetaSys[k] = []byte(v)
				}
				err = x.setIdx(i, *ver)
				return "", err
			}
			x.versions = append(x.versions[:i], x.versions[i+1:]...)
			if fi.MarkDeleted && (fi.VersionPurgeStatus().Empty() || (fi.VersionPurgeStatus() != replication.VersionPurgeComplete)) {
				err = x.addVersion(ventry)
			} else if fi.Deleted && uv.String() == emptyUUID {
				return "", x.addVersion(ventry)
			}
			return "", err
		case ObjectType:
			if updateVersion && !fi.Deleted {
				ver, err := x.getIdx(i)
				if err != nil {
					return "", err
				}
				ver.ObjectV2.MetaSys[VersionPurgeStatusKey] = []byte(fi.ReplicationState.VersionPurgeStatusInternal)
				for k, v := range fi.ReplicationState.ResetStatusesMap {
					ver.ObjectV2.MetaSys[k] = []byte(v)
				}
				err = x.setIdx(i, *ver)
				return uuid.UUID(ver.ObjectV2.DataDir).String(), err
			}
		}
	}

	for i, version := range x.versions {
		if version.header.Type != ObjectType || version.header.VersionID != uv {
			continue
		}
		ver, err := x.getIdx(i)
		if err != nil {
			return "", err
		}
		switch {
		case fi.ExpireRestored:
			ver.ObjectV2.RemoveRestoreHdrs()
			err = x.setIdx(i, *ver)
		case fi.TransitionStatus == lifecycle.TransitionComplete:
			ver.ObjectV2.SetTransition(fi)
			ver.ObjectV2.ResetInlineData()
			err = x.setIdx(i, *ver)
		default:
			x.versions = append(x.versions[:i], x.versions[i+1:]...)
			// if uv has tiered content we add a
			// free-version to track it for
			// asynchronous deletion via scanner.
			if freeVersion, toFree := ver.ObjectV2.InitFreeVersion(fi); toFree {
				err = x.addVersion(freeVersion)
			}
		}

		if fi.Deleted {
			err = x.addVersion(ventry)
		}
		if x.SharedDataDirCount(ver.ObjectV2.VersionID, ver.ObjectV2.DataDir) > 0 {
			// Found that another version references the same dataDir
			// we shouldn't remove it, and only remove the version instead
			return "", nil
		}
		return uuid.UUID(ver.ObjectV2.DataDir).String(), err
	}

	if fi.Deleted {
		err = x.addVersion(ventry)
		return "", err
	}
	return "", errFileVersionNotFound
}

// xlMetaDataDirDecoder is a shallow decoder for decoding object datadir only.
type xlMetaDataDirDecoder struct {
	ObjectV2 *struct {
		DataDir [16]byte `msg:"DDir"` // Data dir ID
	} `msg:"V2Obj,omitempty"`
}

// UpdateObjectVersion updates metadata and modTime for a given
// versionID, NOTE: versionID must be valid and should exist -
// and must not be a DeleteMarker or legacy object, if no
// versionID is specified 'null' versionID is updated instead.
//
// It is callers responsibility to set correct versionID, this
// function shouldn't be further extended to update immutable
// values such as ErasureInfo, ChecksumInfo.
//
// Metadata is only updated to new values, existing values
// stay as is, if you wish to update all values you should
// update all metadata freshly before calling this function
// in-case you wish to clear existing metadata.
func (x *xlMetaV2) UpdateObjectVersion(fi FileInfo) error {
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

	for i, version := range x.versions {
		switch version.header.Type {
		case LegacyType, DeleteType:
			if version.header.VersionID == uv {
				return errMethodNotAllowed
			}
		case ObjectType:
			if version.header.VersionID == uv {
				ver, err := x.getIdx(i)
				if err != nil {
					return err
				}
				for k, v := range fi.Metadata {
					if len(k) > len(ReservedMetadataPrefixLower) && strings.EqualFold(k[:len(ReservedMetadataPrefixLower)], ReservedMetadataPrefixLower) {
						ver.ObjectV2.MetaSys[k] = []byte(v)
					} else {
						ver.ObjectV2.MetaUser[k] = v
					}
				}
				if !fi.ModTime.IsZero() {
					ver.ObjectV2.ModTime = fi.ModTime.UnixNano()
				}
				return x.setIdx(i, *ver)
			}
		}
	}

	return errFileVersionNotFound
}

// AddVersion adds a new version
func (x *xlMetaV2) AddVersion(fi FileInfo) error {
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

	ventry := xlMetaV2Version{
		WrittenByVersion: globalVersionUnix,
	}

	if fi.Deleted {
		ventry.Type = DeleteType
		ventry.DeleteMarker = &xlMetaV2DeleteMarker{
			VersionID: uv,
			ModTime:   fi.ModTime.UnixNano(),
			MetaSys:   make(map[string][]byte),
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
			PartETags:          nil,
			PartSizes:          make([]int64, len(fi.Parts)),
			PartActualSizes:    make([]int64, len(fi.Parts)),
			MetaSys:            make(map[string][]byte),
			MetaUser:           make(map[string]string, len(fi.Metadata)),
		}
		for i := range fi.Parts {
			// Only add etags if any.
			if fi.Parts[i].ETag != "" {
				ventry.ObjectV2.PartETags = make([]string, len(fi.Parts))
				break
			}
		}
		for i := range fi.Parts {
			// Only add indices if any.
			if len(fi.Parts[i].Index) > 0 {
				ventry.ObjectV2.PartIndices = make([][]byte, len(fi.Parts))
				break
			}
		}
		for i := range fi.Erasure.Distribution {
			ventry.ObjectV2.ErasureDist[i] = uint8(fi.Erasure.Distribution[i])
		}

		for i := range fi.Parts {
			ventry.ObjectV2.PartSizes[i] = fi.Parts[i].Size
			if len(ventry.ObjectV2.PartETags) > 0 && fi.Parts[i].ETag != "" {
				ventry.ObjectV2.PartETags[i] = fi.Parts[i].ETag
			}
			ventry.ObjectV2.PartNumbers[i] = fi.Parts[i].Number
			ventry.ObjectV2.PartActualSizes[i] = fi.Parts[i].ActualSize
			if len(ventry.ObjectV2.PartIndices) > i {
				ventry.ObjectV2.PartIndices[i] = fi.Parts[i].Index
			}
		}

		tierFVIDKey := ReservedMetadataPrefixLower + tierFVID
		tierFVMarkerKey := ReservedMetadataPrefixLower + tierFVMarker
		for k, v := range fi.Metadata {
			if len(k) > len(ReservedMetadataPrefixLower) && strings.EqualFold(k[:len(ReservedMetadataPrefixLower)], ReservedMetadataPrefixLower) {
				// Skip tierFVID, tierFVMarker keys; it's used
				// only for creating free-version.
				// Also skip xMinIOHealing, xMinIODataMov as used only in RenameData
				switch k {
				case tierFVIDKey, tierFVMarkerKey, xMinIOHealing, xMinIODataMov:
					continue
				}

				ventry.ObjectV2.MetaSys[k] = []byte(v)
			} else {
				ventry.ObjectV2.MetaUser[k] = v
			}
		}

		// If asked to save data.
		if len(fi.Data) > 0 || fi.Size == 0 {
			x.data.replace(fi.VersionID, fi.Data)
		}

		if fi.TransitionStatus != "" {
			ventry.ObjectV2.MetaSys[metaTierStatus] = []byte(fi.TransitionStatus)
		}
		if fi.TransitionedObjName != "" {
			ventry.ObjectV2.MetaSys[metaTierObjName] = []byte(fi.TransitionedObjName)
		}
		if fi.TransitionVersionID != "" {
			ventry.ObjectV2.MetaSys[metaTierVersionID] = []byte(fi.TransitionVersionID)
		}
		if fi.TransitionTier != "" {
			ventry.ObjectV2.MetaSys[metaTierName] = []byte(fi.TransitionTier)
		}
		if len(fi.Checksum) > 0 {
			ventry.ObjectV2.MetaSys[ReservedMetadataPrefixLower+"crc"] = fi.Checksum
		}
	}

	if !ventry.Valid() {
		return errors.New("internal error: invalid version entry generated")
	}

	// Check if we should replace first.
	for i := range x.versions {
		if x.versions[i].header.VersionID != uv {
			continue
		}
		switch x.versions[i].header.Type {
		case LegacyType:
			// This would convert legacy type into new ObjectType
			// this means that we are basically purging the `null`
			// version of the object.
			return x.setIdx(i, ventry)
		case ObjectType:
			return x.setIdx(i, ventry)
		case DeleteType:
			// Allowing delete marker to replaced with proper
			// object data type as well, this is not S3 complaint
			// behavior but kept here for future flexibility.
			return x.setIdx(i, ventry)
		}
	}

	// We did not find it, add it.
	return x.addVersion(ventry)
}

func (x *xlMetaV2) SharedDataDirCount(versionID [16]byte, dataDir [16]byte) int {
	// v2 object is inlined, if it is skip dataDir share check.
	if x.data.entries() > 0 && x.data.find(uuid.UUID(versionID).String()) != nil {
		return 0
	}
	var sameDataDirCount int
	var decoded xlMetaDataDirDecoder
	for _, version := range x.versions {
		if version.header.Type != ObjectType || version.header.VersionID == versionID || !version.header.UsesDataDir() {
			continue
		}
		_, err := decoded.UnmarshalMsg(version.meta)
		if err != nil || decoded.ObjectV2 == nil || decoded.ObjectV2.DataDir != dataDir {
			continue
		}
		sameDataDirCount++
	}
	return sameDataDirCount
}

func (x *xlMetaV2) SharedDataDirCountStr(versionID, dataDir string) int {
	var (
		uv   uuid.UUID
		ddir uuid.UUID
		err  error
	)
	if versionID == nullVersionID {
		versionID = ""
	}
	if versionID != "" {
		uv, err = uuid.Parse(versionID)
		if err != nil {
			return 0
		}
	}
	ddir, err = uuid.Parse(dataDir)
	if err != nil {
		return 0
	}
	return x.SharedDataDirCount(uv, ddir)
}

// AddLegacy adds a legacy version, is only called when no prior
// versions exist, safe to use it by only one function in xl-storage(RenameData)
func (x *xlMetaV2) AddLegacy(m *xlMetaV1Object) error {
	if !m.valid() {
		return errFileCorrupt
	}
	m.VersionID = nullVersionID

	return x.addVersion(xlMetaV2Version{ObjectV1: m, Type: LegacyType, WrittenByVersion: globalVersionUnix})
}

// ToFileInfo converts xlMetaV2 into a common FileInfo datastructure
// for consumption across callers.
func (x xlMetaV2) ToFileInfo(volume, path, versionID string, inclFreeVers, allParts bool) (fi FileInfo, err error) {
	var uv uuid.UUID
	if versionID != "" && versionID != nullVersionID {
		uv, err = uuid.Parse(versionID)
		if err != nil {
			storageLogIf(GlobalContext, fmt.Errorf("invalid versionID specified %s", versionID))
			return fi, errFileVersionNotFound
		}
	}
	var succModTime int64
	isLatest := true
	nonFreeVersions := len(x.versions)

	var (
		freeFi    FileInfo
		freeFound bool
	)
	found := false
	for _, ver := range x.versions {
		header := &ver.header
		// skip listing free-version unless explicitly requested via versionID
		if header.FreeVersion() {
			nonFreeVersions--
			// remember the latest free version; will return this FileInfo if no non-free version remain
			var freeVersion xlMetaV2Version
			if inclFreeVers && !freeFound {
				// ignore unmarshalling errors, will return errFileNotFound in that case
				if _, err := freeVersion.unmarshalV(x.metaV, ver.meta); err == nil {
					if freeFi, err = freeVersion.ToFileInfo(volume, path, allParts); err == nil {
						freeFi.IsLatest = true // when this is returned, it would be the latest free version remaining.
						freeFound = true
					}
				}
			}

			if header.VersionID != uv {
				continue
			}
		}
		if found {
			continue
		}

		// We need a specific version, skip...
		if versionID != "" && uv != header.VersionID {
			isLatest = false
			succModTime = header.ModTime
			continue
		}

		// We found what we need.
		found = true
		var version xlMetaV2Version
		if _, err := version.unmarshalV(x.metaV, ver.meta); err != nil {
			return fi, err
		}
		if fi, err = version.ToFileInfo(volume, path, allParts); err != nil {
			return fi, err
		}
		fi.IsLatest = isLatest
		if succModTime != 0 {
			fi.SuccessorModTime = time.Unix(0, succModTime)
		}
	}
	if !found {
		if versionID == "" {
			if inclFreeVers && nonFreeVersions == 0 {
				if freeFound {
					return freeFi, nil
				}
			}
			return FileInfo{}, errFileNotFound
		}

		return FileInfo{}, errFileVersionNotFound
	}
	fi.NumVersions = nonFreeVersions
	return fi, err
}

// ListVersions lists current versions, and current deleted
// versions returns error for unexpected entries.
// showPendingDeletes is set to true if ListVersions needs to list objects marked deleted
// but waiting to be replicated
func (x xlMetaV2) ListVersions(volume, path string, allParts bool) ([]FileInfo, error) {
	versions := make([]FileInfo, 0, len(x.versions))
	var err error

	var dst xlMetaV2Version
	for _, version := range x.versions {
		_, err = dst.unmarshalV(x.metaV, version.meta)
		if err != nil {
			return versions, err
		}
		fi, err := dst.ToFileInfo(volume, path, allParts)
		if err != nil {
			return versions, err
		}
		fi.NumVersions = len(x.versions)
		versions = append(versions, fi)
	}

	for i := range versions {
		versions[i].NumVersions = len(versions)
		if i > 0 {
			versions[i].SuccessorModTime = versions[i-1].ModTime
		}
	}
	if len(versions) > 0 {
		versions[0].IsLatest = true
	}
	return versions, nil
}

// mergeXLV2Versions will merge all versions, typically from different disks
// that have at least quorum entries in all metas.
// Each version slice should be sorted.
// Quorum must be the minimum number of matching metadata files.
// Quorum should be > 1 and <= len(versions).
// If strict is set to false, entries that match type
func mergeXLV2Versions(quorum int, strict bool, requestedVersions int, versions ...[]xlMetaV2ShallowVersion) (merged []xlMetaV2ShallowVersion) {
	if quorum <= 0 {
		quorum = 1
	}
	if len(versions) < quorum || len(versions) == 0 {
		return nil
	}
	if len(versions) == 1 {
		return versions[0]
	}
	if quorum == 1 {
		// No need for non-strict checks if quorum is 1.
		strict = true
	}
	// Shallow copy input
	versions = append(make([][]xlMetaV2ShallowVersion, 0, len(versions)), versions...)

	var nVersions int // captures all non-free versions

	// Our result
	merged = make([]xlMetaV2ShallowVersion, 0, len(versions[0]))
	tops := make([]xlMetaV2ShallowVersion, len(versions))
	for {
		// Step 1 create slice with all top versions.
		tops = tops[:0]
		var topSig xlMetaV2VersionHeader
		consistent := true // Are all signatures consistent (shortcut)
		for _, vers := range versions {
			if len(vers) == 0 {
				consistent = false
				continue
			}
			ver := vers[0]
			if len(tops) == 0 {
				consistent = true
				topSig = ver.header
			} else {
				consistent = consistent && ver.header == topSig
			}
			tops = append(tops, vers[0])
		}

		// Check if done...
		if len(tops) < quorum {
			// We couldn't gather enough for quorum
			break
		}

		var latest xlMetaV2ShallowVersion
		if consistent {
			// All had the same signature, easy.
			latest = tops[0]
			merged = append(merged, latest)

			// Calculate latest 'n' non-free versions.
			if !latest.header.FreeVersion() {
				nVersions++
			}
		} else {
			// Find latest.
			var latestCount int
			for i, ver := range tops {
				if ver.header == latest.header {
					latestCount++
					continue
				}
				if i == 0 || ver.header.sortsBefore(latest.header) {
					switch {
					case i == 0 || latestCount == 0:
						latestCount = 1
					case !strict && ver.header.matchesNotStrict(latest.header):
						latestCount++
					default:
						latestCount = 1
					}
					latest = ver
					continue
				}

				// Mismatch, but older.
				if latestCount > 0 && !strict && ver.header.matchesNotStrict(latest.header) {
					latestCount++
					continue
				}
				if latestCount > 0 && ver.header.VersionID == latest.header.VersionID {
					// Version IDs match, but otherwise unable to resolve.
					// We are either strict, or don't have enough information to match.
					// Switch to a pure counting algo.
					x := make(map[xlMetaV2VersionHeader]int, len(tops))
					for _, a := range tops {
						if a.header.VersionID != ver.header.VersionID {
							continue
						}
						if !strict {
							// we must match EC, when we are not strict.
							if !a.header.matchesEC(ver.header) {
								continue
							}

							a.header.Signature = [4]byte{}
						}
						x[a.header]++
					}
					latestCount = 0
					for k, v := range x {
						if v < latestCount {
							continue
						}
						if v == latestCount && latest.header.sortsBefore(k) {
							// Tiebreak, use sort.
							continue
						}
						for _, a := range tops {
							hdr := a.header
							if !strict {
								hdr.Signature = [4]byte{}
							}
							if hdr == k {
								latest = a
							}
						}
						latestCount = v
					}
					break
				}
			}
			if latestCount >= quorum {
				merged = append(merged, latest)

				// Calculate latest 'n' non-free versions.
				if !latest.header.FreeVersion() {
					nVersions++
				}
			}
		}

		// Remove from all streams up until latest modtime or if selected.
		for i, vers := range versions {
			for _, ver := range vers {
				// Truncate later modtimes, not selected.
				if ver.header.ModTime > latest.header.ModTime {
					versions[i] = versions[i][1:]
					continue
				}
				// Truncate matches
				if ver.header == latest.header {
					versions[i] = versions[i][1:]
					continue
				}

				// Truncate non-empty version and type matches
				if latest.header.VersionID == ver.header.VersionID {
					versions[i] = versions[i][1:]
					continue
				}
				// Skip versions with version id we already emitted.
				for _, mergedV := range merged {
					if ver.header.VersionID == mergedV.header.VersionID {
						versions[i] = versions[i][1:]
						continue
					}
				}
				// Keep top entry (and remaining)...
				break
			}
		}

		if requestedVersions > 0 && requestedVersions == nVersions {
			merged = append(merged, versions[0]...)
			break
		}
	}

	// Sanity check. Enable if duplicates show up.
	if false {
		found := make(map[[16]byte]struct{})
		for _, ver := range merged {
			if _, ok := found[ver.header.VersionID]; ok {
				panic("found dupe")
			}
			found[ver.header.VersionID] = struct{}{}
		}
	}
	return merged
}

type xlMetaBuf []byte

// ToFileInfo converts xlMetaV2 into a common FileInfo datastructure
// for consumption across callers.
func (x xlMetaBuf) ToFileInfo(volume, path, versionID string, allParts bool) (fi FileInfo, err error) {
	var uv uuid.UUID
	if versionID != "" && versionID != nullVersionID {
		uv, err = uuid.Parse(versionID)
		if err != nil {
			storageLogIf(GlobalContext, fmt.Errorf("invalid versionID specified %s", versionID))
			return fi, errFileVersionNotFound
		}
	}
	versions, headerV, metaV, buf, err := decodeXLHeaders(x)
	if err != nil {
		return fi, err
	}
	var header xlMetaV2VersionHeader
	var succModTime int64
	isLatest := true
	nonFreeVersions := versions
	found := false
	err = decodeVersions(buf, versions, func(idx int, hdr, meta []byte) error {
		if _, err := header.unmarshalV(headerV, hdr); err != nil {
			return err
		}

		// skip listing free-version unless explicitly requested via versionID
		if header.FreeVersion() {
			nonFreeVersions--
			if header.VersionID != uv {
				return nil
			}
		}
		if found {
			return nil
		}

		// We need a specific version, skip...
		if versionID != "" && uv != header.VersionID {
			isLatest = false
			succModTime = header.ModTime
			return nil
		}

		// We found what we need.
		found = true
		var version xlMetaV2Version
		if _, err := version.unmarshalV(metaV, meta); err != nil {
			return err
		}
		if fi, err = version.ToFileInfo(volume, path, allParts); err != nil {
			return err
		}
		fi.IsLatest = isLatest
		if succModTime != 0 {
			fi.SuccessorModTime = time.Unix(0, succModTime)
		}
		return nil
	})
	if !found {
		if versionID == "" {
			return FileInfo{}, errFileNotFound
		}

		return FileInfo{}, errFileVersionNotFound
	}
	fi.NumVersions = nonFreeVersions
	return fi, err
}

// ListVersions lists current versions, and current deleted
// versions returns error for unexpected entries.
// showPendingDeletes is set to true if ListVersions needs to list objects marked deleted
// but waiting to be replicated
func (x xlMetaBuf) ListVersions(volume, path string, allParts bool) ([]FileInfo, error) {
	vers, _, metaV, buf, err := decodeXLHeaders(x)
	if err != nil {
		return nil, err
	}
	var succModTime time.Time
	isLatest := true
	dst := make([]FileInfo, 0, vers)
	var xl xlMetaV2Version
	err = decodeVersions(buf, vers, func(idx int, hdr, meta []byte) error {
		if _, err := xl.unmarshalV(metaV, meta); err != nil {
			return err
		}
		if !xl.Valid() {
			return errFileCorrupt
		}
		fi, err := xl.ToFileInfo(volume, path, allParts)
		if err != nil {
			return err
		}
		fi.IsLatest = isLatest
		fi.SuccessorModTime = succModTime
		fi.NumVersions = vers
		isLatest = false
		succModTime = xl.getModTime()

		dst = append(dst, fi)
		return nil
	})
	return dst, err
}

// IsLatestDeleteMarker returns true if latest version is a deletemarker or there are no versions.
// If any error occurs false is returned.
func (x xlMetaBuf) IsLatestDeleteMarker() bool {
	vers, headerV, _, buf, err := decodeXLHeaders(x)
	if err != nil {
		return false
	}
	if vers == 0 {
		return true
	}
	isDeleteMarker := false

	_ = decodeVersions(buf, vers, func(idx int, hdr, _ []byte) error {
		var xl xlMetaV2VersionHeader
		if _, err := xl.unmarshalV(headerV, hdr); err != nil {
			return errDoneForNow
		}
		isDeleteMarker = xl.Type == DeleteType
		return errDoneForNow
	})
	return isDeleteMarker
}

// AllHidden returns true are no versions that would show up in a listing (ie all free markers)
// Optionally also return early if top is a delete marker.
func (x xlMetaBuf) AllHidden(topDeleteMarker bool) bool {
	vers, headerV, _, buf, err := decodeXLHeaders(x)
	if err != nil {
		return false
	}
	if vers == 0 {
		return true
	}
	hidden := true

	var xl xlMetaV2VersionHeader
	_ = decodeVersions(buf, vers, func(idx int, hdr, _ []byte) error {
		if _, err := xl.unmarshalV(headerV, hdr); err != nil {
			return errDoneForNow
		}
		if topDeleteMarker && idx == 0 && xl.Type == DeleteType {
			hidden = true
			return errDoneForNow
		}
		if !xl.FreeVersion() {
			hidden = false
			return errDoneForNow
		}
		// Check next version
		return nil
	})
	return hidden
}
