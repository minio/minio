/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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

// Contains code that is
//
// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zipindex

import (
	"os"
	"time"
)

// Compression methods.
const (
	Store   uint16 = 0 // no compression
	Deflate uint16 = 8 // DEFLATE compressed
)

const (
	fileHeaderSignature      = 0x04034b50
	directoryHeaderSignature = 0x02014b50
	directory64LocSignature  = 0x07064b50
	directory64EndSignature  = 0x06064b50
	fileHeaderLen            = 30 // + filename + extra
	directoryHeaderLen       = 46 // + filename + extra + comment
	directoryEndLen          = 22 // + comment
	directory64LocLen        = 20 //
	directory64EndLen        = 56 // + extra
	dataDescriptorLen        = 16

	// Constants for the first byte in CreatorVersion.
	creatorFAT    = 0
	creatorUnix   = 3
	creatorNTFS   = 11
	creatorVFAT   = 14
	creatorMacOSX = 19

	// Version numbers.

	// Limits for non zip64 files.
	uint32max = (1 << 32) - 1

	// Extra header IDs.
	//
	// IDs 0..31 are reserved for official use by PKWARE.
	// IDs above that range are defined by third-party vendors.
	// Since ZIP lacked high precision timestamps (nor a official specification
	// of the timezone used for the date fields), many competing extra fields
	// have been invented. Pervasive use effectively makes them "official".
	//
	// See http://mdfs.net/Docs/Comp/Archiving/Zip/ExtraField
	zip64ExtraID       = 0x0001 // Zip64 extended information
	ntfsExtraID        = 0x000a // NTFS
	unixExtraID        = 0x000d // UNIX
	extTimeExtraID     = 0x5455 // Extended timestamp
	infoZipUnixExtraID = 0x5855 // Info-ZIP Unix extension
)

// fileHeader describes a file within a zip file.
// See the zip spec for details.
type fileHeader struct {
	// Name is the name of the file.
	//
	// It must be a relative path, not start with a drive letter (such as "C:"),
	// and must use forward slashes instead of back slashes. A trailing slash
	// indicates that this file is a directory and should have no data.
	//
	// When reading zip files, the Name field is populated from
	// the zip file directly and is not validated for correctness.
	// It is the caller's responsibility to sanitize it as
	// appropriate, including canonicalizing slash directions,
	// validating that paths are relative, and preventing path
	// traversal through filenames ("../../../").
	Name string

	// Comment is any arbitrary user-defined string shorter than 64KiB.
	Comment string

	// NonUTF8 indicates that Name and Comment are not encoded in UTF-8.
	//
	// By specification, the only other encoding permitted should be CP-437,
	// but historically many ZIP readers interpret Name and Comment as whatever
	// the system's local character encoding happens to be.
	//
	// This flag should only be set if the user intends to encode a non-portable
	// ZIP file for a specific localized region. Otherwise, the Writer
	// automatically sets the ZIP format's UTF-8 flag for valid UTF-8 strings.
	NonUTF8 bool

	CreatorVersion uint16
	ReaderVersion  uint16
	Flags          uint16

	// Method is the compression method. If zero, Store is used.
	Method uint16

	// Modified is the modified time of the file.
	//
	// When reading, an extended timestamp is preferred over the legacy MS-DOS
	// date field, and the offset between the times is used as the timezone.
	// If only the MS-DOS date is present, the timezone is assumed to be UTC.
	//
	// When writing, an extended timestamp (which is timezone-agnostic) is
	// always emitted. The legacy MS-DOS date field is encoded according to the
	// location of the Modified time.
	Modified     time.Time
	ModifiedTime uint16 // Deprecated: Legacy MS-DOS date; use Modified instead.
	ModifiedDate uint16 // Deprecated: Legacy MS-DOS time; use Modified instead.

	CRC32              uint32
	CompressedSize     uint32 // Deprecated: Use CompressedSize64 instead.
	UncompressedSize   uint32 // Deprecated: Use UncompressedSize64 instead.
	CompressedSize64   uint64
	UncompressedSize64 uint64
	Extra              []byte
	ExternalAttrs      uint32 // Meaning depends on CreatorVersion

	headerOffset int64
}

type directoryEnd struct {
	diskNbr            uint32 // unused
	dirDiskNbr         uint32 // unused
	dirRecordsThisDisk uint64 // unused
	directoryRecords   uint64
	directorySize      uint64
	directoryOffset    uint64 // relative to file
	commentLen         uint16
	comment            string
}

// timeZone returns a *time.Location based on the provided offset.
// If the offset is non-sensible, then this uses an offset of zero.
func timeZone(offset time.Duration) *time.Location {
	const (
		minOffset   = -12 * time.Hour  // E.g., Baker island at -12:00
		maxOffset   = +14 * time.Hour  // E.g., Line island at +14:00
		offsetAlias = 15 * time.Minute // E.g., Nepal at +5:45
	)
	offset = offset.Round(offsetAlias)
	if offset < minOffset || maxOffset < offset {
		offset = 0
	}
	return time.FixedZone("", int(offset/time.Second))
}

// msDosTimeToTime converts an MS-DOS date and time into a time.Time.
// The resolution is 2s.
// See: https://msdn.microsoft.com/en-us/library/ms724247(v=VS.85).aspx
func msDosTimeToTime(dosDate, dosTime uint16) time.Time {
	return time.Date(
		// date bits 0-4: day of month; 5-8: month; 9-15: years since 1980
		int(dosDate>>9+1980),
		time.Month(dosDate>>5&0xf),
		int(dosDate&0x1f),

		// time bits 0-4: second/2; 5-10: minute; 11-15: hour
		int(dosTime>>11),
		int(dosTime>>5&0x3f),
		int(dosTime&0x1f*2),
		0, // nanoseconds

		time.UTC,
	)
}

// Mode returns the permission and mode bits for the FileHeader.
func (h *fileHeader) Mode() (mode os.FileMode) {
	switch h.CreatorVersion >> 8 {
	case creatorUnix, creatorMacOSX:
		mode = unixModeToFileMode(h.ExternalAttrs >> 16)
	case creatorNTFS, creatorVFAT, creatorFAT:
		mode = msdosModeToFileMode(h.ExternalAttrs)
	}
	if len(h.Name) > 0 && h.Name[len(h.Name)-1] == '/' {
		mode |= os.ModeDir
	}
	return mode
}

// ModTime returns the modification time in UTC using the legacy
// ModifiedDate and ModifiedTime fields.
//
// Deprecated: Use Modified instead.
func (h *fileHeader) ModTime() time.Time {
	return msDosTimeToTime(h.ModifiedDate, h.ModifiedTime)
}

// isZip64 reports whether the file size exceeds the 32 bit limit
func (h *fileHeader) isZip64() bool {
	return h.CompressedSize64 >= uint32max || h.UncompressedSize64 >= uint32max
}

func msdosModeToFileMode(m uint32) (mode os.FileMode) {
	if m&msdosDir != 0 {
		mode = os.ModeDir | 0777
	} else {
		mode = 0666
	}
	if m&msdosReadOnly != 0 {
		mode &^= 0222
	}
	return mode
}

const (
	// Unix constants. The specification doesn't mention them,
	// but these seem to be the values agreed on by tools.
	sIfmt   = 0xf000
	sIfsock = 0xc000
	sIflnk  = 0xa000
	sIfreg  = 0x8000
	sIfblk  = 0x6000
	sIfdir  = 0x4000
	sIfchr  = 0x2000
	sIfifo  = 0x1000
	sIsuid  = 0x800
	sIsgid  = 0x400
	sIsvtx  = 0x200

	msdosDir      = 0x10
	msdosReadOnly = 0x01
)

func unixModeToFileMode(m uint32) os.FileMode {
	mode := os.FileMode(m & 0777)
	switch m & sIfmt {
	case sIfblk:
		mode |= os.ModeDevice
	case sIfchr:
		mode |= os.ModeDevice | os.ModeCharDevice
	case sIfdir:
		mode |= os.ModeDir
	case sIfifo:
		mode |= os.ModeNamedPipe
	case sIflnk:
		mode |= os.ModeSymlink
	case sIfreg:
		// nothing to do
	case sIfsock:
		mode |= os.ModeSocket
	}
	if m&sIsgid != 0 {
		mode |= os.ModeSetgid
	}
	if m&sIsuid != 0 {
		mode |= os.ModeSetuid
	}
	if m&sIsvtx != 0 {
		mode |= os.ModeSticky
	}
	return mode
}
