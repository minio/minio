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
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tinylib/msgp/msgp"

	"github.com/google/uuid"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
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
	xlVersionMinor = 1
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
	// data will be one or more versions indexed by storage dir.
	// To remove all data set to nil.
	data xlMetaInlineData `msg:"-"`
}

type xlMetaInlineData []byte

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
		return err
	}
	for i := uint32(0); i < sz; i++ {
		var key []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			return fmt.Errorf("xlMetaInlineData: key %d is length 0", i)
		}
		_, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return err
		}
	}
	return nil
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
	payload := make([]byte, 1, plSize)
	payload[0] = xlMetaInlineDataVer
	payload = msgp.AppendMapHeader(payload, uint32(len(keys)))
	for i := range keys {
		payload = msgp.AppendStringFromBytes(payload, keys[i])
		payload = msgp.AppendBytes(payload, vals[i])
	}
	*x = payload
	if err := x.validate(); err != nil {
		panic(err)
	}
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
		plSize += len(foundKey) + len(foundVal) + msgp.StringPrefixSize + msgp.ArrayHeaderSize
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
	payload := make([]byte, 1, plSize)
	payload[0] = xlMetaInlineDataVer
	payload = msgp.AppendMapHeader(payload, uint32(len(keys)))
	for i := range keys {
		payload = msgp.AppendStringFromBytes(payload, keys[i])
		payload = msgp.AppendBytes(payload, vals[i])
	}
	*x = payload
	return true
}

// remove will remove a key.
// Returns whether the key was found.
func (x *xlMetaInlineData) remove(key string) bool {
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
		if string(foundKey) != key {
			plSize += len(foundKey) + msgp.StringPrefixSize + msgp.ArrayHeaderSize + len(foundKey) + len(foundVal)
			keys = append(keys, foundKey)
			vals = append(vals, foundVal)
		} else {
			found = true
		}
	}
	// If not found, just return.
	if !found {
		return false
	}
	// If none left...
	if len(keys) == 0 {
		*x = nil
		return true
	}

	// Reserialize...
	payload := make([]byte, 1, plSize)
	payload[0] = xlMetaInlineDataVer
	payload = msgp.AppendMapHeader(payload, uint32(len(keys)))
	for i := range keys {
		payload = msgp.AppendStringFromBytes(payload, keys[i])
		payload = msgp.AppendBytes(payload, vals[i])
	}
	*x = payload
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
	buf, _, minor, err := checkXL2V1(buf)
	if err != nil {
		return errFileCorrupt
	}
	switch minor {
	case 0:
		_, err = z.UnmarshalMsg(buf)
		if err != nil {
			return errFileCorrupt
		}
		return nil
	case 1:
		v, buf, err := msgp.ReadBytesZC(buf)
		if err != nil {
			return errFileCorrupt
		}
		_, err = z.UnmarshalMsg(v)
		if err != nil {
			return errFileCorrupt
		}
		// Add remaining data.
		z.data = nil
		if len(buf) > 0 {
			z.data = buf
			if err := z.data.validate(); err != nil {
				return errFileCorrupt
			}
		}
	default:
		return errors.New("unknown metadata version")
	}
	return nil
}

// AppendTo will marshal the data in z and append it to the provided slice.
func (z *xlMetaV2) AppendTo(dst []byte) ([]byte, error) {
	sz := len(xlHeader) + len(xlVersionCurrent) + msgp.ArrayHeaderSize + z.Msgsize() + len(z.data) + len(dst)
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

	return append(dst, z.data...), nil
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
		if fi.Data != nil || fi.Size == 0 {
			z.data.replace(dd.String(), fi.Data)
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
		if equals(k, xhttp.AmzMetaUnencryptedContentLength, xhttp.AmzMetaUnencryptedContentMD5) {
			continue
		}

		fi.Metadata[k] = v
	}
	for k, v := range j.MetaSys {
		switch {
		case equals(k, ReservedMetadataPrefixLower+"transition-status"):
			fi.TransitionStatus = string(v)
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
				if fi.TransitionStatus != "" {
					z.Versions[i].ObjectV1.Meta[ReservedMetadataPrefixLower+"transition-status"] = fi.TransitionStatus
					return uuid.UUID(version.ObjectV2.DataDir).String(), len(z.Versions) == 0, nil
				}

				z.Versions = append(z.Versions[:i], z.Versions[i+1:]...)
				if fi.Deleted {
					z.Versions = append(z.Versions, ventry)
				}
				return version.ObjectV1.DataDir, len(z.Versions) == 0, nil
			}
		case DeleteType:
			if bytes.Equal(version.DeleteMarker.VersionID[:], uv[:]) {
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
			if bytes.Equal(version.ObjectV2.VersionID[:], uv[:]) && updateVersion {
				z.Versions[i].ObjectV2.MetaSys[VersionPurgeStatusKey] = []byte(fi.VersionPurgeStatus)
				return "", len(z.Versions) == 0, nil
			}
		}
	}

	findDataDir := func(dataDir [16]byte, versions []xlMetaV2Version) int {
		var sameDataDirCount int
		for _, version := range versions {
			switch version.Type {
			case ObjectType:
				if version.ObjectV2.DataDir == dataDir {
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
			if version.ObjectV2.VersionID == uv {
				if fi.TransitionStatus != "" {
					z.Versions[i].ObjectV2.MetaSys[ReservedMetadataPrefixLower+"transition-status"] = []byte(fi.TransitionStatus)
					return uuid.UUID(version.ObjectV2.DataDir).String(), len(z.Versions) == 0, nil
				}
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
// showPendingDeletes is set to true if ListVersions needs to list objects marked deleted
// but waiting to be replicated
func (z xlMetaV2) ListVersions(volume, path string) ([]FileInfo, time.Time, error) {
	var versions []FileInfo
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

	sort.Sort(versionsSorter(versions))

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
