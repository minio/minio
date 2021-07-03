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
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/minio/minio/internal/bucket/lifecycle"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/tinylib/msgp/msgp"
)

var (
	// XL header specifies the format
	xlHeader = [4]byte{'X', 'L', '2', ' '}

	// Current version being written.
	xlVersionCurrent [4]byte
)

const (
	// Breaking changes.
	// Newer versions cannot be read by older software.
	// This will prevent downgrades to incompatible versions.
	xlVersionMajor = 1

	// Non breaking changes.
	// Bumping this is informational, but should be done
	// if any change is made to the data stored, bumping this
	// will allow to detect the exact version later.
	xlVersionMinor = 2
)

func init() {
	binary.LittleEndian.PutUint16(xlVersionCurrent[0:2], xlVersionMajor)
	binary.LittleEndian.PutUint16(xlVersionCurrent[2:4], xlVersionMinor)
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
	VersionID [16]byte          `json:"ID" msg:"ID"`                               // Version ID for delete marker
	ModTime   int64             `json:"MTime" msg:"MTime"`                         // Object delete marker modified time
	MetaSys   map[string][]byte `json:"MetaSys,omitempty" msg:"MetaSys,omitempty"` // Delete marker internal metadata
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

// xlMetaV2 - object meta structure defines the format and list of
// the journals for the object.
type xlMetaV2 struct {
	Versions []xlMetaV2Version `json:"Versions" msg:"Versions"`

	// data will contain raw data if any.
	// data will be one or more versions indexed by versionID.
	// To remove all data set to nil.
	data xlMetaInlineData `msg:"-"`
}

// xlMetaInlineData is serialized data in [string][]byte pairs.
//
//msgp:ignore xlMetaInlineData
type xlMetaInlineData []byte

// xlMetaInlineDataVer indicates the vesrion of the inline data structure.
const xlMetaInlineDataVer = 1

// versionOK returns whether the version is ok.
func (x xlMetaInlineData) versionOK() bool {
	if len(x) == 0 {
		return true
	}
	return x[0] > 0 && x[0] <= xlMetaInlineDataVer
}

// afterVersion returns the payload after the version, if any.
func (x xlMetaInlineData) afterVersion() []byte {
	if len(x) == 0 {
		return x
	}
	return x[1:]
}

// find the data with key s.
// Returns nil if not for or an error occurs.
func (x xlMetaInlineData) find(key string) []byte {
	if len(x) == 0 || !x.versionOK() {
		return nil
	}
	sz, buf, err := msgp.ReadMapHeaderBytes(x.afterVersion())
	if err != nil || sz == 0 {
		return nil
	}
	for i := uint32(0); i < sz; i++ {
		var found []byte
		found, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil || sz == 0 {
			return nil
		}
		if string(found) == key {
			val, _, _ := msgp.ReadBytesZC(buf)
			return val
		}
		// Skip it
		_, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return nil
		}
	}
	return nil
}

// validate checks if the data is valid.
// It does not check integrity of the stored data.
func (x xlMetaInlineData) validate() error {
	if len(x) == 0 {
		return nil
	}

	if !x.versionOK() {
		return fmt.Errorf("xlMetaInlineData: unknown version 0x%x", x[0])
	}

	sz, buf, err := msgp.ReadMapHeaderBytes(x.afterVersion())
	if err != nil {
		return fmt.Errorf("xlMetaInlineData: %w", err)
	}

	for i := uint32(0); i < sz; i++ {
		var key []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			return fmt.Errorf("xlMetaInlineData: %w", err)
		}
		if len(key) == 0 {
			return fmt.Errorf("xlMetaInlineData: key %d is length 0", i)
		}
		_, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return fmt.Errorf("xlMetaInlineData: %w", err)
		}
	}

	return nil
}

// repair will copy all seemingly valid data entries from a corrupted set.
// This does not ensure that data is correct, but will allow all operations to complete.
func (x *xlMetaInlineData) repair() {
	data := *x
	if len(data) == 0 {
		return
	}

	if !data.versionOK() {
		*x = nil
		return
	}

	sz, buf, err := msgp.ReadMapHeaderBytes(data.afterVersion())
	if err != nil {
		*x = nil
		return
	}

	// Remove all current data
	keys := make([][]byte, 0, sz)
	vals := make([][]byte, 0, sz)
	for i := uint32(0); i < sz; i++ {
		var key, val []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			break
		}
		if len(key) == 0 {
			break
		}
		val, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			break
		}
		keys = append(keys, key)
		vals = append(vals, val)
	}
	x.serialize(-1, keys, vals)
}

// validate checks if the data is valid.
// It does not check integrity of the stored data.
func (x xlMetaInlineData) list() ([]string, error) {
	if len(x) == 0 {
		return nil, nil
	}
	if !x.versionOK() {
		return nil, errors.New("xlMetaInlineData: unknown version")
	}

	sz, buf, err := msgp.ReadMapHeaderBytes(x.afterVersion())
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, sz)
	for i := uint32(0); i < sz; i++ {
		var key []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			return keys, err
		}
		if len(key) == 0 {
			return keys, fmt.Errorf("xlMetaInlineData: key %d is length 0", i)
		}
		keys = append(keys, string(key))
		// Skip data...
		_, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return keys, err
		}
	}
	return keys, nil
}

// serialize will serialize the provided keys and values.
// The function will panic if keys/value slices aren't of equal length.
// Payload size can give an indication of expected payload size.
// If plSize is <= 0 it will be calculated.
func (x *xlMetaInlineData) serialize(plSize int, keys [][]byte, vals [][]byte) {
	if len(keys) != len(vals) {
		panic(fmt.Errorf("xlMetaInlineData.serialize: keys/value number mismatch"))
	}
	if len(keys) == 0 {
		*x = nil
		return
	}
	if plSize <= 0 {
		plSize = 1 + msgp.MapHeaderSize
		for i := range keys {
			plSize += len(keys[i]) + len(vals[i]) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
		}
	}
	payload := make([]byte, 1, plSize)
	payload[0] = xlMetaInlineDataVer
	payload = msgp.AppendMapHeader(payload, uint32(len(keys)))
	for i := range keys {
		payload = msgp.AppendStringFromBytes(payload, keys[i])
		payload = msgp.AppendBytes(payload, vals[i])
	}
	*x = payload
}

// entries returns the number of entries in the data.
func (x xlMetaInlineData) entries() int {
	if len(x) == 0 || !x.versionOK() {
		return 0
	}
	sz, _, _ := msgp.ReadMapHeaderBytes(x.afterVersion())
	return int(sz)
}

// replace will add or replace a key/value pair.
func (x *xlMetaInlineData) replace(key string, value []byte) {
	in := x.afterVersion()
	sz, buf, _ := msgp.ReadMapHeaderBytes(in)
	keys := make([][]byte, 0, sz+1)
	vals := make([][]byte, 0, sz+1)

	// Version plus header...
	plSize := 1 + msgp.MapHeaderSize
	replaced := false
	for i := uint32(0); i < sz; i++ {
		var found, foundVal []byte
		var err error
		found, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			break
		}
		foundVal, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			break
		}
		plSize += len(found) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
		keys = append(keys, found)
		if string(found) == key {
			vals = append(vals, value)
			plSize += len(value)
			replaced = true
		} else {
			vals = append(vals, foundVal)
			plSize += len(foundVal)
		}
	}

	// Add one more.
	if !replaced {
		keys = append(keys, []byte(key))
		vals = append(vals, value)
		plSize += len(key) + len(value) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
	}

	// Reserialize...
	x.serialize(plSize, keys, vals)
}

// rename will rename a key.
// Returns whether the key was found.
func (x *xlMetaInlineData) rename(oldKey, newKey string) bool {
	in := x.afterVersion()
	sz, buf, _ := msgp.ReadMapHeaderBytes(in)
	keys := make([][]byte, 0, sz)
	vals := make([][]byte, 0, sz)

	// Version plus header...
	plSize := 1 + msgp.MapHeaderSize
	found := false
	for i := uint32(0); i < sz; i++ {
		var foundKey, foundVal []byte
		var err error
		foundKey, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			break
		}
		foundVal, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			break
		}
		plSize += len(foundVal) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
		vals = append(vals, foundVal)
		if string(foundKey) != oldKey {
			keys = append(keys, foundKey)
			plSize += len(foundKey)
		} else {
			keys = append(keys, []byte(newKey))
			plSize += len(newKey)
			found = true
		}
	}
	// If not found, just return.
	if !found {
		return false
	}

	// Reserialize...
	x.serialize(plSize, keys, vals)
	return true
}

// remove will remove one or more keys.
// Returns true if any key was found.
func (x *xlMetaInlineData) remove(keys ...string) bool {
	in := x.afterVersion()
	sz, buf, _ := msgp.ReadMapHeaderBytes(in)
	newKeys := make([][]byte, 0, sz)
	newVals := make([][]byte, 0, sz)
	var removeKey func(s []byte) bool

	// Copy if big number of compares...
	if len(keys) > 5 && sz > 5 {
		mKeys := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			mKeys[key] = struct{}{}
		}
		removeKey = func(s []byte) bool {
			_, ok := mKeys[string(s)]
			return ok
		}
	} else {
		removeKey = func(s []byte) bool {
			for _, key := range keys {
				if key == string(s) {
					return true
				}
			}
			return false
		}
	}

	// Version plus header...
	plSize := 1 + msgp.MapHeaderSize
	found := false
	for i := uint32(0); i < sz; i++ {
		var foundKey, foundVal []byte
		var err error
		foundKey, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			break
		}
		foundVal, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			break
		}
		if !removeKey(foundKey) {
			plSize += msgp.StringPrefixSize + msgp.ArrayHeaderSize + len(foundKey) + len(foundVal)
			newKeys = append(newKeys, foundKey)
			newVals = append(newVals, foundVal)
		} else {
			found = true
		}
	}
	// If not found, just return.
	if !found {
		return false
	}
	// If none left...
	if len(newKeys) == 0 {
		*x = nil
		return true
	}

	// Reserialize...
	x.serialize(plSize, newKeys, newVals)
	return true
}

// xlMetaV2TrimData will trim any data from the metadata without unmarshalling it.
// If any error occurs the unmodified data is returned.
func xlMetaV2TrimData(buf []byte) []byte {
	metaBuf, min, maj, err := checkXL2V1(buf)
	if err != nil {
		return buf
	}
	if maj == 1 && min < 1 {
		// First version to carry data.
		return buf
	}
	// Skip header
	_, metaBuf, err = msgp.ReadBytesZC(metaBuf)
	if err != nil {
		logger.LogIf(GlobalContext, err)
		return buf
	}
	// Skip CRC
	if maj > 1 || min >= 2 {
		_, metaBuf, err = msgp.ReadUint32Bytes(metaBuf)
		logger.LogIf(GlobalContext, err)
	}
	//   =  input - current pos
	ends := len(buf) - len(metaBuf)
	if ends > len(buf) {
		return buf
	}

	return buf[:ends]
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
// Note that references to the incoming buffer may be kept as data.
func (z *xlMetaV2) Load(buf []byte) error {
	buf, major, minor, err := checkXL2V1(buf)
	if err != nil {
		return fmt.Errorf("xlMetaV2.Load %w", err)
	}
	switch major {
	case 1:
		switch minor {
		case 0:
			_, err = z.UnmarshalMsg(buf)
			if err != nil {
				return fmt.Errorf("xlMetaV2.Load %w", err)
			}
			return nil
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

			if _, err = z.UnmarshalMsg(v); err != nil {
				return fmt.Errorf("xlMetaV2.Load version(%d), vLen(%d), %w", minor, len(v), err)
			}
			// Add remaining data.
			z.data = buf
			if err = z.data.validate(); err != nil {
				z.data.repair()
				logger.Info("xlMetaV2.Load: data validation failed: %v. %d entries after repair", err, z.data.entries())
			}
		default:
			return errors.New("unknown minor metadata version")
		}
	default:
		return errors.New("unknown major metadata version")
	}
	return nil
}

// AppendTo will marshal the data in z and append it to the provided slice.
func (z *xlMetaV2) AppendTo(dst []byte) ([]byte, error) {
	sz := len(xlHeader) + len(xlVersionCurrent) + msgp.ArrayHeaderSize + z.Msgsize() + len(z.data) + len(dst) + msgp.Uint32Size
	if cap(dst) < sz {
		buf := make([]byte, len(dst), sz)
		copy(buf, dst)
		dst = buf
	}
	if err := z.data.validate(); err != nil {
		return nil, err
	}

	dst = append(dst, xlHeader[:]...)
	dst = append(dst, xlVersionCurrent[:]...)
	// Add "bin 32" type header to always have enough space.
	// We will fill out the correct size when we know it.
	dst = append(dst, 0xc6, 0, 0, 0, 0)
	dataOffset := len(dst)
	dst, err := z.MarshalMsg(dst)
	if err != nil {
		return nil, err
	}

	// Update size...
	binary.BigEndian.PutUint32(dst[dataOffset-4:dataOffset], uint32(len(dst)-dataOffset))

	// Add CRC of metadata.
	dst = msgp.AppendUint32(dst, uint32(xxhash.Sum64(dst[dataOffset:])))
	return append(dst, z.data...), nil
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
func (z *xlMetaV2) UpdateObjectVersion(fi FileInfo) error {
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

	for i, version := range z.Versions {
		if !version.Valid() {
			return errFileCorrupt
		}
		switch version.Type {
		case LegacyType:
			if version.ObjectV1.VersionID == fi.VersionID {
				return errMethodNotAllowed
			}
		case ObjectType:
			if version.ObjectV2.VersionID == uv {
				for k, v := range fi.Metadata {
					if strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower) {
						z.Versions[i].ObjectV2.MetaSys[k] = []byte(v)
					} else {
						z.Versions[i].ObjectV2.MetaUser[k] = v
					}
				}
				if !fi.ModTime.IsZero() {
					z.Versions[i].ObjectV2.ModTime = fi.ModTime.UnixNano()
				}
				return nil
			}
		case DeleteType:
			return errMethodNotAllowed
		}
	}

	return errFileVersionNotFound
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

		// If asked to save data.
		if len(fi.Data) > 0 || fi.Size == 0 {
			z.data.replace(fi.VersionID, fi.Data)
		}

		if fi.TransitionStatus != "" {
			ventry.ObjectV2.MetaSys[ReservedMetadataPrefixLower+TransitionStatus] = []byte(fi.TransitionStatus)
		}
		if fi.TransitionedObjName != "" {
			ventry.ObjectV2.MetaSys[ReservedMetadataPrefixLower+TransitionedObjectName] = []byte(fi.TransitionedObjName)
		}
		if fi.TransitionVersionID != "" {
			ventry.ObjectV2.MetaSys[ReservedMetadataPrefixLower+TransitionedVersionID] = []byte(fi.TransitionVersionID)
		}
		if fi.TransitionTier != "" {
			ventry.ObjectV2.MetaSys[ReservedMetadataPrefixLower+TransitionTier] = []byte(fi.TransitionTier)
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
			if version.ObjectV2.VersionID == uv {
				z.Versions[i] = ventry
				return nil
			}
		case DeleteType:
			// Allowing delete marker to replaced with an proper
			// object data type as well, this is not S3 complaint
			// behavior but kept here for future flexibility.
			if version.DeleteMarker.VersionID == uv {
				z.Versions[i] = ventry
				return nil
			}
		}
	}

	z.Versions = append(z.Versions, ventry)
	return nil
}

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
	for k, v := range j.MetaSys {
		switch {
		case equals(k, xhttp.AmzBucketReplicationStatus):
			fi.DeleteMarkerReplicationStatus = string(v)
		case equals(k, VersionPurgeStatusKey):
			fi.VersionPurgeStatus = VersionPurgeStatusType(string(v))
		}
	}
	return fi, nil
}

// UsesDataDir returns true if this object version uses its data directory for
// its contents and false otherwise.
func (j *xlMetaV2Object) UsesDataDir() bool {
	// Skip if this version is not transitioned, i.e it uses its data directory.
	if !bytes.Equal(j.MetaSys[ReservedMetadataPrefixLower+TransitionStatus], []byte(lifecycle.TransitionComplete)) {
		return true
	}

	// Check if this transitioned object has been restored on disk.
	return isRestoredObjectOnDisk(j.MetaUser)
}

func (j xlMetaV2Object) ToFileInfo(volume, path string) (FileInfo, error) {
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
		if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
			continue
		}

		fi.Metadata[k] = v
	}
	for k, v := range j.MetaSys {
		switch {
		case equals(k, VersionPurgeStatusKey):
			fi.VersionPurgeStatus = VersionPurgeStatusType(string(v))
		case strings.HasPrefix(strings.ToLower(k), ReservedMetadataPrefixLower):
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

	if st, ok := j.MetaSys[ReservedMetadataPrefixLower+TransitionStatus]; ok {
		fi.TransitionStatus = string(st)
	}
	if o, ok := j.MetaSys[ReservedMetadataPrefixLower+TransitionedObjectName]; ok {
		fi.TransitionedObjName = string(o)
	}
	if rv, ok := j.MetaSys[ReservedMetadataPrefixLower+TransitionedVersionID]; ok {
		fi.TransitionVersionID = string(rv)
	}
	if sc, ok := j.MetaSys[ReservedMetadataPrefixLower+TransitionTier]; ok {
		fi.TransitionTier = string(sc)
	}
	return fi, nil
}

func (z *xlMetaV2) SharedDataDirCountStr(versionID, dataDir string) int {
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
	return z.SharedDataDirCount(uv, ddir)
}

func (z *xlMetaV2) SharedDataDirCount(versionID [16]byte, dataDir [16]byte) int {
	// v2 object is inlined, if it is skip dataDir share check.
	if z.data.find(uuid.UUID(versionID).String()) != nil {
		return 0
	}
	var sameDataDirCount int
	for _, version := range z.Versions {
		switch version.Type {
		case ObjectType:
			if version.ObjectV2.VersionID == versionID {
				continue
			}
			if version.ObjectV2.DataDir != dataDir {
				continue
			}
			if version.ObjectV2.UsesDataDir() {
				sameDataDirCount++
			}
		}
	}
	return sameDataDirCount
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
	var err error
	if fi.VersionID != "" {
		uv, err = uuid.Parse(fi.VersionID)
		if err != nil {
			return "", false, errFileVersionNotFound
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
		}
		if !ventry.Valid() {
			return "", false, errors.New("internal error: invalid version entry generated")
		}
	}
	updateVersion := false
	if fi.VersionPurgeStatus.Empty() && (fi.DeleteMarkerReplicationStatus == "REPLICA" || fi.DeleteMarkerReplicationStatus == "") {
		updateVersion = fi.MarkDeleted
	} else {
		// for replication scenario
		if fi.Deleted && fi.VersionPurgeStatus != Complete {
			if !fi.VersionPurgeStatus.Empty() || fi.DeleteMarkerReplicationStatus != "" {
				updateVersion = true
			}
		}
		// object or delete-marker versioned delete is not complete
		if !fi.VersionPurgeStatus.Empty() && fi.VersionPurgeStatus != Complete {
			updateVersion = true
		}
	}
	if fi.Deleted {
		if fi.DeleteMarkerReplicationStatus != "" {
			ventry.DeleteMarker.MetaSys[xhttp.AmzBucketReplicationStatus] = []byte(fi.DeleteMarkerReplicationStatus)
		}
		if !fi.VersionPurgeStatus.Empty() {
			ventry.DeleteMarker.MetaSys[VersionPurgeStatusKey] = []byte(fi.VersionPurgeStatus)
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
			if version.DeleteMarker.VersionID == uv {
				if updateVersion {
					if len(z.Versions[i].DeleteMarker.MetaSys) == 0 {
						z.Versions[i].DeleteMarker.MetaSys = make(map[string][]byte)
					}
					delete(z.Versions[i].DeleteMarker.MetaSys, xhttp.AmzBucketReplicationStatus)
					delete(z.Versions[i].DeleteMarker.MetaSys, VersionPurgeStatusKey)
					if fi.DeleteMarkerReplicationStatus != "" {
						z.Versions[i].DeleteMarker.MetaSys[xhttp.AmzBucketReplicationStatus] = []byte(fi.DeleteMarkerReplicationStatus)
					}
					if !fi.VersionPurgeStatus.Empty() {
						z.Versions[i].DeleteMarker.MetaSys[VersionPurgeStatusKey] = []byte(fi.VersionPurgeStatus)
					}
				} else {
					z.Versions = append(z.Versions[:i], z.Versions[i+1:]...)
					if fi.MarkDeleted && (fi.VersionPurgeStatus.Empty() || (fi.VersionPurgeStatus != Complete)) {
						z.Versions = append(z.Versions, ventry)
					}
				}
				return "", len(z.Versions) == 0, nil
			}
		case ObjectType:
			if version.ObjectV2.VersionID == uv && updateVersion {
				z.Versions[i].ObjectV2.MetaSys[VersionPurgeStatusKey] = []byte(fi.VersionPurgeStatus)
				return "", len(z.Versions) == 0, nil
			}
		}
	}

	for i, version := range z.Versions {
		if !version.Valid() {
			return "", false, errFileCorrupt
		}
		switch version.Type {
		case ObjectType:
			if version.ObjectV2.VersionID == uv {
				switch {
				case fi.ExpireRestored:
					delete(z.Versions[i].ObjectV2.MetaUser, xhttp.AmzRestore)
					delete(z.Versions[i].ObjectV2.MetaUser, xhttp.AmzRestoreExpiryDays)
					delete(z.Versions[i].ObjectV2.MetaUser, xhttp.AmzRestoreRequestDate)
				case fi.TransitionStatus == lifecycle.TransitionComplete:
					z.Versions[i].ObjectV2.MetaSys[ReservedMetadataPrefixLower+TransitionStatus] = []byte(fi.TransitionStatus)
					z.Versions[i].ObjectV2.MetaSys[ReservedMetadataPrefixLower+TransitionedObjectName] = []byte(fi.TransitionedObjName)
					z.Versions[i].ObjectV2.MetaSys[ReservedMetadataPrefixLower+TransitionedVersionID] = []byte(fi.TransitionVersionID)
					z.Versions[i].ObjectV2.MetaSys[ReservedMetadataPrefixLower+TransitionTier] = []byte(fi.TransitionTier)
				default:
					z.Versions = append(z.Versions[:i], z.Versions[i+1:]...)
				}

				if fi.Deleted {
					z.Versions = append(z.Versions, ventry)
				}
				if z.SharedDataDirCount(version.ObjectV2.VersionID, version.ObjectV2.DataDir) > 0 {
					// Found that another version references the same dataDir
					// we shouldn't remove it, and only remove the version instead
					return "", len(z.Versions) == 0, nil
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
// showPendingDeletes is set to true if ListVersions needs to list objects marked deleted
// but waiting to be replicated
func (z xlMetaV2) ListVersions(volume, path string) ([]FileInfo, time.Time, error) {
	versions := make([]FileInfo, 0, len(z.Versions))
	var err error

	for _, version := range z.Versions {
		if !version.Valid() {
			return nil, time.Time{}, errFileCorrupt
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
			return nil, time.Time{}, err
		}
		versions = append(versions, fi)
	}

	versionsSorter(versions).sort()

	for i := range versions {
		versions[i].NumVersions = len(versions)
		if i > 0 {
			versions[i].SuccessorModTime = versions[i-1].ModTime
		}
	}

	versions[0].IsLatest = true
	return versions, versions[0].ModTime, nil
}

func getModTimeFromVersion(v xlMetaV2Version) time.Time {
	switch v.Type {
	case ObjectType:
		return time.Unix(0, v.ObjectV2.ModTime)
	case DeleteType:
		return time.Unix(0, v.DeleteMarker.ModTime)
	case LegacyType:
		return v.ObjectV1.Stat.ModTime
	}
	return time.Time{}
}

// ToFileInfo converts xlMetaV2 into a common FileInfo datastructure
// for consumption across callers.
func (z xlMetaV2) ToFileInfo(volume, path, versionID string) (fi FileInfo, err error) {
	var uv uuid.UUID
	if versionID != "" && versionID != nullVersionID {
		uv, err = uuid.Parse(versionID)
		if err != nil {
			logger.LogIf(GlobalContext, fmt.Errorf("invalid versionID specified %s", versionID))
			return FileInfo{}, errFileVersionNotFound
		}
	}

	for _, version := range z.Versions {
		if !version.Valid() {
			logger.LogIf(GlobalContext, fmt.Errorf("invalid version detected %#v", version))
			if versionID == "" {
				return FileInfo{}, errFileNotFound
			}
			return FileInfo{}, errFileVersionNotFound

		}
	}

	orderedVersions := make([]xlMetaV2Version, len(z.Versions))
	copy(orderedVersions, z.Versions)

	sort.Slice(orderedVersions, func(i, j int) bool {
		mtime1 := getModTimeFromVersion(orderedVersions[i])
		mtime2 := getModTimeFromVersion(orderedVersions[j])
		return mtime1.After(mtime2)
	})

	if versionID == "" {
		if len(orderedVersions) >= 1 {
			switch orderedVersions[0].Type {
			case ObjectType:
				fi, err = orderedVersions[0].ObjectV2.ToFileInfo(volume, path)
			case DeleteType:
				fi, err = orderedVersions[0].DeleteMarker.ToFileInfo(volume, path)
			case LegacyType:
				fi, err = orderedVersions[0].ObjectV1.ToFileInfo(volume, path)
			}
			fi.IsLatest = true
			fi.NumVersions = len(orderedVersions)
			return fi, err
		}
		return FileInfo{}, errFileNotFound
	}

	var foundIndex = -1

	for i := range orderedVersions {
		switch orderedVersions[i].Type {
		case ObjectType:
			if bytes.Equal(orderedVersions[i].ObjectV2.VersionID[:], uv[:]) {
				fi, err = orderedVersions[i].ObjectV2.ToFileInfo(volume, path)
				foundIndex = i
				break
			}
		case LegacyType:
			if orderedVersions[i].ObjectV1.VersionID == versionID {
				fi, err = orderedVersions[i].ObjectV1.ToFileInfo(volume, path)
				foundIndex = i
				break
			}
		case DeleteType:
			if bytes.Equal(orderedVersions[i].DeleteMarker.VersionID[:], uv[:]) {
				fi, err = orderedVersions[i].DeleteMarker.ToFileInfo(volume, path)
				foundIndex = i
				break
			}
		}
	}
	if err != nil {
		return fi, err
	}

	if foundIndex >= 0 {
		// A version is found, fill dynamic fields
		fi.IsLatest = foundIndex == 0
		fi.NumVersions = len(z.Versions)
		if foundIndex > 0 {
			fi.SuccessorModTime = getModTimeFromVersion(orderedVersions[foundIndex-1])
		}
		return fi, nil
	}

	if versionID == "" {
		return FileInfo{}, errFileNotFound
	}

	return FileInfo{}, errFileVersionNotFound
}

// readXLMetaNoData will load the metadata, but skip data segments.
// This should only be used when data is never interesting.
// If data is not xlv2, it is returned in full.
func readXLMetaNoData(r io.Reader, size int64) ([]byte, error) {
	// Read at most this much on initial read.
	const readDefault = 4 << 10
	initial := size
	hasFull := true
	if initial > readDefault {
		initial = readDefault
		hasFull = false
	}

	buf := make([]byte, initial)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("readXLMetaNoData.ReadFull: %w", err)
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
		buf = append(buf, make([]byte, extra)...)
		_, err := io.ReadFull(r, buf[has:])
		if err != nil {
			if err == io.EOF {
				// Returned if we read nothing.
				return io.ErrUnexpectedEOF
			}
			return fmt.Errorf("readXLMetaNoData.readMore: %w", err)
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
		case 1, 2:
			sz, tmp, err := msgp.ReadBytesHeader(tmp)
			if err != nil {
				return nil, err
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
			wantMax := want + msgp.Uint32Size
			if wantMax > size {
				wantMax = size
			}
			if err := readMore(wantMax); err != nil {
				return nil, err
			}

			tmp = buf[want:]
			_, after, err := msgp.ReadUint32Bytes(tmp)
			if err != nil {
				return nil, err
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
