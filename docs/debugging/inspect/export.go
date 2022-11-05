// Copyright (c) 2015-2022 MinIO, Inc.
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

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	json "github.com/minio/colorjson"

	"github.com/klauspost/compress/zip"
	"github.com/tinylib/msgp/msgp"
)

func inspectToExportType(downloadPath string, datajson bool) error {
	decode := func(r io.Reader, file string) ([]byte, error) {
		b, e := ioutil.ReadAll(r)
		if e != nil {
			return nil, e
		}
		b, _, minor, e := checkXL2V1(b)
		if e != nil {
			return nil, e
		}

		buf := bytes.NewBuffer(nil)
		var data xlMetaInlineData
		switch minor {
		case 0:
			_, e = msgp.CopyToJSON(buf, bytes.NewReader(b))
			if e != nil {
				return nil, e
			}
		case 1, 2:
			v, b, e := msgp.ReadBytesZC(b)
			if e != nil {
				return nil, e
			}
			if _, nbuf, e := msgp.ReadUint32Bytes(b); e == nil {
				// Read metadata CRC (added in v2, ignore if not found)
				b = nbuf
			}

			_, e = msgp.CopyToJSON(buf, bytes.NewReader(v))
			if e != nil {
				return nil, e
			}
			data = b
		case 3:
			v, b, e := msgp.ReadBytesZC(b)
			if e != nil {
				return nil, e
			}
			if _, nbuf, e := msgp.ReadUint32Bytes(b); e == nil {
				// Read metadata CRC (added in v2, ignore if not found)
				b = nbuf
			}

			nVers, v, e := decodeXLHeaders(v)
			if e != nil {
				return nil, e
			}
			type version struct {
				Idx      int
				Header   json.RawMessage
				Metadata json.RawMessage
			}
			versions := make([]version, nVers)
			e = decodeVersions(v, nVers, func(idx int, hdr, meta []byte) error {
				var header xlMetaV2VersionHeaderV2
				if _, e := header.UnmarshalMsg(hdr); e != nil {
					return e
				}
				b, e := header.MarshalJSON()
				if e != nil {
					return e
				}
				var buf bytes.Buffer
				if _, e := msgp.UnmarshalAsJSON(&buf, meta); e != nil {
					return e
				}
				versions[idx] = version{
					Idx:      idx,
					Header:   b,
					Metadata: buf.Bytes(),
				}
				return nil
			})
			if e != nil {
				return nil, e
			}
			enc := json.NewEncoder(buf)
			if e := enc.Encode(struct {
				Versions []version
			}{Versions: versions}); e != nil {
				return nil, e
			}
			data = b
		default:
			return nil, fmt.Errorf("unknown metadata version %d", minor)
		}

		if datajson {
			b, e := data.json()
			if e != nil {
				return nil, e
			}
			buf = bytes.NewBuffer(b)
		}

		return buf.Bytes(), nil
	}

	fmt.Println("{")

	hasWritten := false
	var r io.Reader
	var sz int64
	f, e := os.Open(downloadPath)
	if e != nil {
		return e
	}
	if st, e := f.Stat(); e == nil {
		sz = st.Size()
	}
	defer f.Close()
	r = f

	zr, e := zip.NewReader(r.(io.ReaderAt), sz)
	if e != nil {
		return e
	}
	for _, file := range zr.File {
		if !file.FileInfo().IsDir() && strings.HasSuffix(file.Name, "xl.meta") {
			r, e := file.Open()
			if e != nil {
				return e
			}
			// Quote string...
			b, _ := json.Marshal(file.Name)
			if hasWritten {
				fmt.Print(",\n")
			}
			fmt.Printf("\t%s: ", string(b))

			b, e = decode(r, file.Name)
			if e != nil {
				return e
			}
			fmt.Print(string(b))
			hasWritten = true
		}
	}
	fmt.Println("")
	fmt.Println("}")

	return nil
}

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
func checkXL2V1(buf []byte) (payload []byte, major, minor uint16, e error) {
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

const xlMetaInlineDataVer = 1

type xlMetaInlineData []byte

// afterVersion returns the payload after the version, if any.
func (x xlMetaInlineData) afterVersion() []byte {
	if len(x) == 0 {
		return x
	}
	return x[1:]
}

// versionOK returns whether the version is ok.
func (x xlMetaInlineData) versionOK() bool {
	if len(x) == 0 {
		return true
	}
	return x[0] > 0 && x[0] <= xlMetaInlineDataVer
}

func (x xlMetaInlineData) json() ([]byte, error) {
	if len(x) == 0 {
		return []byte("{}"), nil
	}
	if !x.versionOK() {
		return nil, errors.New("xlMetaInlineData: unknown version")
	}

	sz, buf, e := msgp.ReadMapHeaderBytes(x.afterVersion())
	if e != nil {
		return nil, e
	}
	res := []byte("{")

	for i := uint32(0); i < sz; i++ {
		var key, val []byte
		key, buf, e = msgp.ReadMapKeyZC(buf)
		if e != nil {
			return nil, e
		}
		if len(key) == 0 {
			return nil, fmt.Errorf("xlMetaInlineData: key %d is length 0", i)
		}
		// Skip data...
		val, buf, e = msgp.ReadBytesZC(buf)
		if e != nil {
			return nil, e
		}
		if i > 0 {
			res = append(res, ',')
		}
		s := fmt.Sprintf(`"%s":%d`, string(key), len(val))
		res = append(res, []byte(s)...)
	}
	res = append(res, '}')
	return res, nil
}

const (
	xlHeaderVersion = 2
	xlMetaVersion   = 1
)

func decodeXLHeaders(buf []byte) (versions int, b []byte, e error) {
	hdrVer, buf, e := msgp.ReadUintBytes(buf)
	if e != nil {
		return 0, buf, e
	}
	metaVer, buf, e := msgp.ReadUintBytes(buf)
	if e != nil {
		return 0, buf, e
	}
	if hdrVer > xlHeaderVersion {
		return 0, buf, fmt.Errorf("decodeXLHeaders: Unknown xl header version %d", metaVer)
	}
	if metaVer > xlMetaVersion {
		return 0, buf, fmt.Errorf("decodeXLHeaders: Unknown xl meta version %d", metaVer)
	}
	versions, buf, e = msgp.ReadIntBytes(buf)
	if e != nil {
		return 0, buf, e
	}
	if versions < 0 {
		return 0, buf, fmt.Errorf("decodeXLHeaders: Negative version count %d", versions)
	}
	return versions, buf, nil
}

// decodeVersions will decode a number of versions from a buffer
// and perform a callback for each version in order, newest first.
// Any non-nil error is returned.
func decodeVersions(buf []byte, versions int, fn func(idx int, hdr, meta []byte) error) (e error) {
	var tHdr, tMeta []byte // Zero copy bytes
	for i := 0; i < versions; i++ {
		tHdr, buf, e = msgp.ReadBytesZC(buf)
		if e != nil {
			return e
		}
		tMeta, buf, e = msgp.ReadBytesZC(buf)
		if e != nil {
			return e
		}
		if e = fn(i, tHdr, tMeta); e != nil {
			return e
		}
	}
	return nil
}

type xlMetaV2VersionHeaderV2 struct {
	VersionID [16]byte
	ModTime   int64
	Signature [4]byte
	Type      uint8
	Flags     uint8
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *xlMetaV2VersionHeaderV2) UnmarshalMsg(bts []byte) (o []byte, e error) {
	var zb0001 uint32
	zb0001, bts, e = msgp.ReadArrayHeaderBytes(bts)
	if e != nil {
		e = msgp.WrapError(e)
		return
	}
	if zb0001 != 5 {
		e = msgp.ArrayError{Wanted: 5, Got: zb0001}
		return
	}
	bts, e = msgp.ReadExactBytes(bts, (z.VersionID)[:])
	if e != nil {
		e = msgp.WrapError(e, "VersionID")
		return
	}
	z.ModTime, bts, e = msgp.ReadInt64Bytes(bts)
	if e != nil {
		e = msgp.WrapError(e, "ModTime")
		return
	}
	bts, e = msgp.ReadExactBytes(bts, (z.Signature)[:])
	if e != nil {
		e = msgp.WrapError(e, "Signature")
		return
	}
	{
		var zb0002 uint8
		zb0002, bts, e = msgp.ReadUint8Bytes(bts)
		if e != nil {
			e = msgp.WrapError(e, "Type")
			return
		}
		z.Type = zb0002
	}
	{
		var zb0003 uint8
		zb0003, bts, e = msgp.ReadUint8Bytes(bts)
		if e != nil {
			e = msgp.WrapError(e, "Flags")
			return
		}
		z.Flags = zb0003
	}
	o = bts
	return
}

func (z xlMetaV2VersionHeaderV2) MarshalJSON() (o []byte, e error) {
	tmp := struct {
		VersionID string
		ModTime   time.Time
		Signature string
		Type      uint8
		Flags     uint8
	}{
		VersionID: hex.EncodeToString(z.VersionID[:]),
		ModTime:   time.Unix(0, z.ModTime),
		Signature: hex.EncodeToString(z.Signature[:]),
		Type:      z.Type,
		Flags:     z.Flags,
	}
	return json.Marshal(tmp)
}
