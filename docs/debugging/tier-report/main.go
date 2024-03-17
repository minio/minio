package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/highwayhash"
	"github.com/tidwall/gjson"
	"github.com/tinylib/msgp/msgp"
)

var (
	baseDirStats os.FileInfo
	baseDir      string
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
	sz, buf, err := msgp.ReadMapHeaderBytes(x.afterVersion())
	if err != nil {
		return nil, err
	}
	res := []byte("{")

	for i := uint32(0); i < sz; i++ {
		var key, val []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			return nil, err
		}
		if len(key) == 0 {
			return nil, fmt.Errorf("xlMetaInlineData: key %d is length 0", i)
		}
		// Skip data...
		val, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			res = append(res, ',')
		}
		s := fmt.Sprintf(`"%s": {"bytes": %d`, string(key), len(val))
		// Check bitrot... We should only ever have one block...
		if len(val) >= 32 {
			want := val[:32]
			data := val[32:]
			const magicHighwayHash256Key = "\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0"

			hh, _ := highwayhash.New([]byte(magicHighwayHash256Key))
			hh.Write(data)
			got := hh.Sum(nil)
			if bytes.Equal(want, got) {
				s += ", \"bitrot_valid\": true"
			} else {
				s += ", \"bitrot_valid\": false"
			}
			s += "}"
		}
		res = append(res, []byte(s)...)
	}
	res = append(res, '}')
	return res, nil
}

// files returns files as callback.
func (x xlMetaInlineData) files(fn func(name string, data []byte)) error {
	if len(x) == 0 {
		return nil
	}
	if !x.versionOK() {
		return errors.New("xlMetaInlineData: unknown version")
	}

	sz, buf, err := msgp.ReadMapHeaderBytes(x.afterVersion())
	if err != nil {
		return err
	}

	for i := uint32(0); i < sz; i++ {
		var key, val []byte
		key, buf, err = msgp.ReadMapKeyZC(buf)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			return fmt.Errorf("xlMetaInlineData: key %d is length 0", i)
		}
		// Read data...
		val, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return err
		}
		// Call back.
		fn(string(key), val)
	}
	return nil
}

const (
	xlHeaderVersion = 2
	xlMetaVersion   = 2
)

func decodeXLHeaders(buf []byte) (versions int, b []byte, err error) {
	hdrVer, buf, err := msgp.ReadUintBytes(buf)
	if err != nil {
		return 0, buf, err
	}
	metaVer, buf, err := msgp.ReadUintBytes(buf)
	if err != nil {
		return 0, buf, err
	}
	if hdrVer > xlHeaderVersion {
		return 0, buf, fmt.Errorf("decodeXLHeaders: Unknown xl header version %d", metaVer)
	}
	if metaVer > xlMetaVersion {
		return 0, buf, fmt.Errorf("decodeXLHeaders: Unknown xl meta version %d", metaVer)
	}
	versions, buf, err = msgp.ReadIntBytes(buf)
	if err != nil {
		return 0, buf, err
	}
	if versions < 0 {
		return 0, buf, fmt.Errorf("decodeXLHeaders: Negative version count %d", versions)
	}
	return versions, buf, nil
}

// decodeVersions will decode a number of versions from a buffer
// and perform a callback for each version in order, newest first.
// Any non-nil error is returned.
func decodeVersions(buf []byte, versions int, fn func(idx int, hdr, meta []byte) error) (err error) {
	var tHdr, tMeta []byte // Zero copy bytes
	for i := 0; i < versions; i++ {
		tHdr, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return err
		}
		tMeta, buf, err = msgp.ReadBytesZC(buf)
		if err != nil {
			return err
		}
		if err = fn(i, tHdr, tMeta); err != nil {
			return err
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
func (z *xlMetaV2VersionHeaderV2) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if zb0001 != 5 {
		err = msgp.ArrayError{Wanted: 5, Got: zb0001}
		return
	}
	bts, err = msgp.ReadExactBytes(bts, (z.VersionID)[:])
	if err != nil {
		err = msgp.WrapError(err, "VersionID")
		return
	}
	z.ModTime, bts, err = msgp.ReadInt64Bytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "ModTime")
		return
	}
	bts, err = msgp.ReadExactBytes(bts, (z.Signature)[:])
	if err != nil {
		err = msgp.WrapError(err, "Signature")
		return
	}
	{
		var zb0002 uint8
		zb0002, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Type")
			return
		}
		z.Type = zb0002
	}
	{
		var zb0003 uint8
		zb0003, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err, "Flags")
			return
		}
		z.Flags = zb0003
	}
	o = bts
	return
}

func (z xlMetaV2VersionHeaderV2) MarshalJSON() (o []byte, err error) {
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

func main() {
	baseDir = os.Getenv("MPATH")
	if baseDir == "" {
		baseDir = "/mount"
	}

	decode := func(r io.Reader, file string) ([]byte, error) {
		b, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		b, _, minor, err := checkXL2V1(b)
		if err != nil {
			return nil, err
		}
		buf := bytes.NewBuffer(nil)
		switch minor {
		case 0:
			_, err = msgp.CopyToJSON(buf, bytes.NewReader(b))
			if err != nil {
				return nil, err
			}
		case 1, 2:
			v, b, err := msgp.ReadBytesZC(b)
			if err != nil {
				return nil, err
			}
			if _, nbuf, err := msgp.ReadUint32Bytes(b); err == nil {
				// Read metadata CRC (added in v2, ignore if not found)
				b = nbuf
			}

			_, err = msgp.CopyToJSON(buf, bytes.NewReader(v))
			if err != nil {
				return nil, err
			}
		case 3:
			v, b, err := msgp.ReadBytesZC(b)
			if err != nil {
				return nil, err
			}
			if _, nbuf, err := msgp.ReadUint32Bytes(b); err == nil {
				// Read metadata CRC (added in v2, ignore if not found)
				b = nbuf
			}

			nVers, v, err := decodeXLHeaders(v)
			if err != nil {
				return nil, err
			}
			type version struct {
				Idx      int
				Header   json.RawMessage
				Metadata json.RawMessage
			}
			versions := make([]version, nVers)
			err = decodeVersions(v, nVers, func(idx int, hdr, meta []byte) error {
				var header xlMetaV2VersionHeaderV2
				if _, err := header.UnmarshalMsg(hdr); err != nil {
					return err
				}
				b, err := header.MarshalJSON()
				if err != nil {
					return err
				}
				var buf bytes.Buffer
				if _, err := msgp.UnmarshalAsJSON(&buf, meta); err != nil {
					return err
				}
				versions[idx] = version{
					Idx:      idx,
					Header:   b,
					Metadata: buf.Bytes(),
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			enc := json.NewEncoder(buf)
			if err := enc.Encode(struct {
				Versions []version
			}{Versions: versions}); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown metadata version %d", minor)
		}

		var msi map[string]interface{}
		dec := json.NewDecoder(buf)
		// Use number to preserve integers.
		dec.UseNumber()
		err = dec.Decode(&msi)
		if err != nil {
			return nil, err
		}
		b, err = json.MarshalIndent(msi, "", "  ")
		if err != nil {
			return nil, err
		}
		return b, nil
	}

	var err error
	baseDirStats, err = os.Stat(baseDir)
	if err != nil {
		fmt.Println("ERROR:" + err.Error())
		return
	}
	if !baseDirStats.IsDir() {
		fmt.Println("ERROR: base is not a directory")
		return
	}

	err = filepath.WalkDir(baseDir, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			fmt.Println("ERROR:", path, "// err:", err)
			return nil
		}
		if strings.HasSuffix(path, "xl.meta") {
			r, err := os.Open(path)
			if err != nil {
				return nil
			}

			buf, err := decode(r, path)
			r.Close()
			if err != nil {
				return nil
			}
			u, err := base64.StdEncoding.DecodeString(gjson.GetBytes(buf, "@dig:DDir").Get("0").String())
			if err != nil {
				return nil
			}
			s, err := base64.StdEncoding.DecodeString(gjson.GetBytes(buf, "@dig:MetaSys").Get("0").Get("x-minio-internal-transition-status").String())
			if err != nil {
				return nil
			}
			if string(s) == "" {
				return nil
			}
			t, err := base64.StdEncoding.DecodeString(gjson.GetBytes(buf, "@dig:MetaSys").Get("0").Get("x-minio-internal-transition-tier").String())
			if err != nil {
				return nil
			}
			o, err := base64.StdEncoding.DecodeString(gjson.GetBytes(buf, "@dig:MetaSys").Get("0").Get("x-minio-internal-transitioned-object").String())
			if err != nil {
				return nil
			}
			if string(t) != "" && string(o) != "" {
				dataDir := filepath.Join(filepath.Dir(path), uuid.UUID(u).String())
				st, err := os.Stat(dataDir)
				if err != nil {
					return nil
				}
				if st.IsDir() {
					fmt.Println(dataDir)
				}
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println("ERROR:", err)
	}
}
