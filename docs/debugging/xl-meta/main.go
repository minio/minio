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

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zip"
	"github.com/klauspost/filepathx"
	"github.com/klauspost/reedsolomon"
	"github.com/minio/cli"
	"github.com/minio/highwayhash"
	"github.com/tinylib/msgp/msgp"
)

func main() {
	app := cli.NewApp()
	app.Copyright = "MinIO, Inc."
	app.Usage = "xl.meta to JSON"
	app.HideVersion = true
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .VisibleFlags}}[FLAGS]{{end}} METAFILES...

Multiple files can be added. Files ending in '.zip' will be searched
for 'xl.meta' files. Wildcards are accepted: 'testdir/*.txt' will compress
all files in testdir ending with '.txt', directories can be wildcards
as well. 'testdir/*/*.txt' will match 'testdir/subdir/b.txt', double stars
means full recursive. 'testdir/**/xl.meta' will search for all xl.meta
recursively.

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
`
	isPart := regexp.MustCompile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/part\\.[0-9]+$")

	app.HideHelpCommand = true
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Usage:  "print each file as a separate line without formatting",
			Name:   "ndjson",
			Hidden: true,
		},
		cli.BoolFlag{
			Usage: "display inline data keys and sizes",
			Name:  "data",
		},
		cli.BoolFlag{
			Usage: "export inline data",
			Name:  "export",
		},
		cli.BoolFlag{
			Usage: "combine inline data",
			Name:  "combine",
		},
		cli.BoolFlag{
			Usage: "combine inline data across versions when size matches",
			Name:  "xver",
		},
	}

	app.Action = func(c *cli.Context) error {
		ndjson := c.Bool("ndjson")
		if c.Bool("data") && c.Bool("combine") {
			return errors.New("cannot combine --data and --combine")
		}
		// file / version / file
		filemap := make(map[string]map[string]string)
		foundData := make(map[string][]byte)
		partDataToVerID := make(map[string][2]string)
		var baseName string
		// versionID ->
		combineFiles := make(map[string][]string)
		decode := func(r io.Reader, file string) ([]byte, error) {
			file = strings.ReplaceAll(file, ":", "_")
			b, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			b, _, minor, err := checkXL2V1(b)
			if err != nil {
				return nil, err
			}
			filemap[file] = make(map[string]string)
			buf := bytes.NewBuffer(nil)
			v0 := ""
			var data xlMetaInlineData
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
				data = b
			case 3:
				v, b, err := msgp.ReadBytesZC(b)
				if err != nil {
					return nil, err
				}
				if _, nbuf, err := msgp.ReadUint32Bytes(b); err == nil {
					// Read metadata CRC (added in v2, ignore if not found)
					b = nbuf
				}

				hdr, v, err := decodeXLHeaders(v)
				if err != nil {
					return nil, err
				}
				type version struct {
					Idx      int
					Header   json.RawMessage
					Metadata json.RawMessage
				}
				versions := make([]version, hdr.versions)
				headerVer := hdr.headerVer
				err = decodeVersions(v, hdr.versions, func(idx int, hdr, meta []byte) error {
					var header xlMetaV2VersionHeaderV2
					if _, err := header.UnmarshalMsg(hdr, headerVer); err != nil {
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
					type erasureInfo struct {
						V2Obj *struct {
							EcDist   []int
							EcIndex  int
							EcM      int
							EcN      int
							DDir     []byte
							PartNums []int
							MetaSys  struct {
								Inline []byte `json:"x-minio-internal-inline-data"`
							}
						}
					}
					var ei erasureInfo
					if err := json.Unmarshal(buf.Bytes(), &ei); err == nil && ei.V2Obj != nil {
						verID := uuid.UUID(header.VersionID).String()
						if verID == "00000000-0000-0000-0000-000000000000" {
							// If the version ID is all zeros, use the signature as version ID.
							verID = fmt.Sprintf("null/%08x", header.Signature)
							v0 = verID
						}
						idx := ei.V2Obj.EcIndex
						filemap[file][verID] = fmt.Sprintf("%s/shard-%02d-of-%02d", verID, idx, ei.V2Obj.EcN+ei.V2Obj.EcM)
						filemap[file][verID+".json"] = buf.String()
						for _, i := range ei.V2Obj.PartNums {
							if len(ei.V2Obj.MetaSys.Inline) != 0 {
								break
							}
							file := file
							dataFile := fmt.Sprintf("%s%s/part.%d", strings.TrimSuffix(file, "xl.meta"), uuid.UUID(ei.V2Obj.DDir).String(), i)
							if i > 1 {
								file = fmt.Sprintf("%s/part.%d", file, i)
								filemap[file] = make(map[string]string)
								filemap[file][verID] = fmt.Sprintf("%s/part.%d/shard-%02d-of-%02d", verID, i, idx, ei.V2Obj.EcN+ei.V2Obj.EcM)
								filemap[file][verID+".json"] = buf.String()
							}
							partDataToVerID[dataFile] = [2]string{file, verID}
						}
					} else if err != nil {
						fmt.Println("Error:", err)
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
				data = b
			default:
				return nil, fmt.Errorf("unknown metadata version %d", minor)
			}

			if c.Bool("data") {
				b, err := data.json(true)
				if err != nil {
					return nil, err
				}
				buf = bytes.NewBuffer(b)
			}
			if c.Bool("export") {
				file := file
				if !c.Bool("combine") {
					file = strings.Map(func(r rune) rune {
						switch {
						case r >= 'a' && r <= 'z':
							return r
						case r >= 'A' && r <= 'Z':
							return r
						case r >= '0' && r <= '9':
							return r
						case strings.ContainsAny(string(r), "+=-_()!@."):
							return r
						default:
							return '_'
						}
					}, file)
				}
				if baseName == "" {
					if strings.HasSuffix(file, "/xl.meta") {
						baseName = strings.TrimSuffix(file, "/xl.meta")
						if idx := strings.LastIndexByte(baseName, '/'); idx > 0 {
							baseName = baseName[idx+1:]
						}
					}
				}
				err := data.files(func(name string, data []byte) {
					fn := fmt.Sprintf("%s-%s.data", file, name)
					if c.Bool("combine") {
						if name == "null" {
							name = v0
						}

						f := filemap[file][name]
						if f != "" {
							fn = f + ".data"
							err = os.MkdirAll(filepath.Dir(fn), os.ModePerm)
							if err != nil {
								fmt.Println("MkdirAll:", filepath.Dir(fn), err)
							}
							err = os.WriteFile(fn+".json", []byte(filemap[file][name+".json"]), os.ModePerm)
							combineFiles[name] = append(combineFiles[name], fn)
							if err != nil {
								fmt.Println("WriteFile:", err)
							}
							err = os.WriteFile(filepath.Dir(fn)+"/filename.txt", []byte(file), os.ModePerm)
							if err != nil {
								fmt.Println("combine WriteFile:", err)
							}
						}
					}
					err = os.WriteFile(fn, data, os.ModePerm)
					if err != nil {
						fmt.Println("WriteFile:", err)
					}
				})
				if err != nil {
					return nil, err
				}
			}
			if ndjson {
				return buf.Bytes(), nil
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

		args := c.Args()
		if len(args) == 0 {
			// If no args, assume xl.meta
			args = []string{"xl.meta"}
		}
		var files []string

		for _, pattern := range args {
			if pattern == "-" {
				files = append(files, pattern)
				continue
			}
			found, err := filepathx.Glob(pattern)
			if err != nil {
				return err
			}
			if len(found) == 0 {
				return fmt.Errorf("unable to find file %v", pattern)
			}
			files = append(files, found...)
		}
		if len(files) == 0 {
			return fmt.Errorf("no files found")
		}
		if len(files) > 1 || strings.HasSuffix(files[0], ".zip") {
			ndjson = true
		}

		toPrint := make([]string, 0, 16)
		for _, file := range files {
			var r io.Reader
			var sz int64
			switch file {
			case "-":
				r = os.Stdin
			default:
				f, err := os.Open(file)
				if err != nil {
					return err
				}
				if st, err := f.Stat(); err == nil {
					sz = st.Size()
				}
				defer f.Close()
				r = f
			}
			if strings.HasSuffix(file, ".zip") {
				zr, err := zip.NewReader(r.(io.ReaderAt), sz)
				if err != nil {
					return err
				}
				for _, file := range zr.File {
					if file.FileInfo().IsDir() {
						continue
					}
					if strings.HasSuffix(file.Name, "xl.meta") {
						r, err := file.Open()
						if err != nil {
							return err
						}
						// Quote string...
						b, _ := json.Marshal(file.Name)
						b2, err := decode(r, file.Name)
						if err != nil {
							return err
						}
						var tmp map[string]interface{}
						if err := json.Unmarshal(b2, &tmp); err == nil {
							if b3, err := json.Marshal(tmp); err == nil {
								b2 = b3
							}
						}
						toPrint = append(toPrint, fmt.Sprintf("\t%s: %s", string(b), string(b2)))
					} else if c.Bool("combine") && isPart.MatchString(file.Name) {
						// name := isPart.FindString(file.Name)
						name := strings.ReplaceAll(file.Name, ":", "_")
						r, err := file.Open()
						if err != nil {
							return err
						}
						all, err := io.ReadAll(r)
						if err != nil {
							return err
						}
						foundData[name] = all
					}
				}
			} else {
				b0 := ""
				if ndjson {
					b, _ := json.Marshal(file)
					b0 = fmt.Sprintf("%s: ", string(b))
				}
				b, err := decode(r, file)
				if err != nil {
					return err
				}
				b = bytes.TrimSpace(b)
				if !ndjson {
					b = bytes.TrimFunc(b, func(r rune) bool {
						return r == '{' || r == '}' || r == '\n' || r == '\r'
					})
				}

				toPrint = append(toPrint, fmt.Sprintf("%s%s", b0, string(b)))
			}
		}
		sort.Strings(toPrint)
		fmt.Printf("{\n%s\n}\n", strings.Join(toPrint, ",\n"))
		if c.Bool("combine") {
			for partName, data := range foundData {
				if verid := partDataToVerID[partName]; verid != [2]string{} {
					file := verid[0]
					name := verid[1]
					f := filemap[file][name]
					fn := fmt.Sprintf("%s-%s.data", file, name)
					if f != "" {
						fn = f + ".data"
						err := os.MkdirAll(filepath.Dir(fn), os.ModePerm)
						if err != nil {
							fmt.Println("MkdirAll:", filepath.Dir(fn), err)
						}
						err = os.WriteFile(fn+".json", []byte(filemap[file][name+".json"]), os.ModePerm)
						combineFiles[name] = append(combineFiles[name], fn)
						if err != nil {
							fmt.Println("WriteFile:", err)
						}
						err = os.WriteFile(filepath.Dir(fn)+"/filename.txt", []byte(file), os.ModePerm)
						if err != nil {
							fmt.Println("combine WriteFile:", err)
						}
						fmt.Println("Remapped", partName, "to", fn)
					}
					delete(partDataToVerID, partName)
					err := os.WriteFile(fn, data, os.ModePerm)
					if err != nil {
						fmt.Println("WriteFile:", err)
					}
				}
			}
			if len(partDataToVerID) > 0 {
				fmt.Println("MISSING PART FILES:")
				for k := range partDataToVerID {
					fmt.Println(k)
				}
				fmt.Println("END MISSING PART FILES")
			}
		}

		if len(combineFiles) > 0 {
			if c.Bool("xver") {
				if err := combineCrossVer(combineFiles, baseName); err != nil {
					fmt.Println("ERROR:", err)
				}
			} else {
				for k, v := range combineFiles {
					if err := combine(v, k+"-"+baseName); err != nil {
						fmt.Println("ERROR:", err)
					}
				}
			}
		}

		return nil
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
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

func (x xlMetaInlineData) json(value bool) ([]byte, error) {
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
			if value {
				if utf8.Valid(data) {
					// Encode as JSON string.
					b, err := json.Marshal(string(data))
					if err == nil {
						s += `, "data_string": ` + string(b)
					}
				}
				// Base64 encode.
				s += `, "data_base64": "` + base64.StdEncoding.EncodeToString(data) + `"`
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
	xlHeaderVersion = 3
	xlMetaVersion   = 2
)

type xlHeaders struct {
	versions           int
	headerVer, metaVer uint
}

func decodeXLHeaders(buf []byte) (x xlHeaders, b []byte, err error) {
	x.headerVer, buf, err = msgp.ReadUintBytes(buf)
	if err != nil {
		return x, buf, err
	}
	x.metaVer, buf, err = msgp.ReadUintBytes(buf)
	if err != nil {
		return x, buf, err
	}
	if x.headerVer > xlHeaderVersion {
		return x, buf, fmt.Errorf("decodeXLHeaders: Unknown xl header version %d", x.headerVer)
	}
	if x.metaVer > xlMetaVersion {
		return x, buf, fmt.Errorf("decodeXLHeaders: Unknown xl meta version %d", x.metaVer)
	}
	x.versions, buf, err = msgp.ReadIntBytes(buf)
	if err != nil {
		return x, buf, err
	}
	if x.versions < 0 {
		return x, buf, fmt.Errorf("decodeXLHeaders: Negative version count %d", x.versions)
	}
	return x, buf, nil
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
	EcN, EcM  uint8 // Note that these will be 0/0 for non-v2 objects and older xl.meta
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *xlMetaV2VersionHeaderV2) UnmarshalMsg(bts []byte, hdrVer uint) (o []byte, err error) {
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	want := uint32(5)
	if hdrVer > 2 {
		want += 2
	}
	if zb0001 != want {
		err = msgp.ArrayError{Wanted: want, Got: zb0001}
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
	if hdrVer > 2 {
		// Version 3 has EcM and EcN
		{
			var zb0004 uint8
			zb0004, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "EcN")
				return
			}
			z.EcN = zb0004
		}
		{
			var zb0005 uint8
			zb0005, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "EcM")
				return
			}
			z.EcM = zb0005
		}
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
		EcM, EcN  uint8 // Note that these will be 0/0 for non-v2 objects and older xl.meta
	}{
		VersionID: hex.EncodeToString(z.VersionID[:]),
		ModTime:   time.Unix(0, z.ModTime),
		Signature: hex.EncodeToString(z.Signature[:]),
		Type:      z.Type,
		Flags:     z.Flags,
		EcM:       z.EcM,
		EcN:       z.EcN,
	}
	return json.Marshal(tmp)
}

type mappedData struct {
	mapped, filled             []byte
	size, shards, data, parity int
	parityData                 map[int]map[int][]byte
	blockOffset                int // Offset in bytes to start of block.
	blocks                     int // 0 = one block.
	objSize, partSize          int
	wantMD5                    string
}

func readAndMap(files []string, partNum, blockNum int) (*mappedData, error) {
	var m mappedData
	sort.Strings(files)
	m.parityData = make(map[int]map[int][]byte)
	for _, file := range files {
		meta, err := os.ReadFile(file + ".json")
		if err != nil {
			return nil, err
		}
		type erasureInfo struct {
			V2Obj *struct {
				EcDist    []int
				EcIndex   int
				EcM       int
				EcN       int
				Size      int
				EcBSize   int
				PartNums  []int
				PartSizes []int
				MetaUsr   struct {
					Etag string `json:"etag"`
				}
			}
		}
		var ei erasureInfo
		var idx int
		if err := json.Unmarshal(meta, &ei); err == nil && ei.V2Obj != nil {
			if m.objSize == 0 {
				m.objSize = ei.V2Obj.Size
			}
			m.data = ei.V2Obj.EcM
			m.parity = ei.V2Obj.EcN
			if len(ei.V2Obj.PartNums) == 1 && !strings.ContainsRune(ei.V2Obj.MetaUsr.Etag, '-') {
				m.wantMD5 = ei.V2Obj.MetaUsr.Etag
			}
			if m.shards == 0 {
				m.shards = m.data + m.parity
			}
			idx = ei.V2Obj.EcIndex - 1
			fmt.Println("Read shard", ei.V2Obj.EcIndex, fmt.Sprintf("(%s)", file))
			if ei.V2Obj.Size != m.objSize {
				return nil, fmt.Errorf("size mismatch. Meta size: %d, Prev: %d", ei.V2Obj.Size, m.objSize)
			}
			for i, s := range ei.V2Obj.PartNums {
				if s == partNum {
					m.size = ei.V2Obj.PartSizes[i]
					m.partSize = ei.V2Obj.PartSizes[i]
					break
				}
			}
		} else {
			return nil, err
		}

		offset := ei.V2Obj.EcBSize * blockNum
		if offset >= m.size {
			return nil, fmt.Errorf("block %d out of range. offset %d > size %d", blockNum, offset, m.size)
		}
		m.blockOffset = offset
		m.blocks = (m.size + ei.V2Obj.EcBSize - 1) / ei.V2Obj.EcBSize
		if m.blocks > 0 {
			m.blocks--
		}
		if blockNum < m.blocks {
			m.size = ei.V2Obj.EcBSize
		} else {
			m.size -= offset
		}

		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		if len(b) < 32 {
			return nil, fmt.Errorf("file %s too short", file)
		}

		// Extract block data.
		ssz := shardSize(ei.V2Obj.EcBSize, ei.V2Obj.EcM)
		b, err = bitrot(b, blockNum*ssz, ssz)
		if err != nil {
			return nil, err
		}

		if m.mapped == nil {
			m.mapped = make([]byte, m.size)
			m.filled = make([]byte, m.size)
		}

		set := m.parityData[m.data]
		if set == nil {
			set = make(map[int][]byte)
		}
		set[idx] = b
		m.parityData[m.data] = set

		// Combine
		start := len(b) * idx
		if start >= len(m.mapped) {
			continue
		}
		fmt.Println("Block data size:", m.size, "Shard size", ssz, "Got Shard:", len(b), "Bitrot ok", "Start", start, "End", start+len(b))
		copy(m.mapped[start:], b)
		for j := range b {
			if j+start >= len(m.filled) {
				break
			}
			m.filled[j+start] = 1
		}
	}
	return &m, nil
}

func combine(files []string, out string) error {
	fmt.Printf("Attempting to combine version %q.\n", out)
	m, err := readAndMap(files, 1, 0)
	if err != nil {
		return err
	}
	if m.blocks > 0 {
		// TODO: Support multiple blocks. For now use -xver.
		return fmt.Errorf("multiple blocks found, only one block supported. Try with -xver")
	}
	lastValid := 0
	missing := 0
	for i := range m.filled {
		if m.filled[i] == 1 {
			lastValid = i
		} else {
			missing++
		}
	}
	if missing > 0 && len(m.parityData) > 0 {
		fmt.Println("Attempting to reconstruct using parity sets:")
		for k, v := range m.parityData {
			if missing == 0 {
				break
			}
			fmt.Println("* Setup: Data shards:", k, "- Parity blocks:", m.shards-k)
			rs, err := reedsolomon.New(k, m.shards-k)
			if err != nil {
				return err
			}
			split, err := rs.Split(m.mapped)
			if err != nil {
				return err
			}
			splitFilled, err := rs.Split(m.filled)
			if err != nil {
				return err
			}
			ok := len(splitFilled)
			for i, sh := range splitFilled {
				for _, v := range sh {
					if v == 0 {
						split[i] = nil
						ok--
						break
					}
				}
			}
			hasParity := 0
			for idx, sh := range v {
				split[idx] = sh
				if idx >= k && len(sh) > 0 {
					hasParity++
				}
			}
			fmt.Printf("Have %d complete remapped data shards and %d complete parity shards. ", ok, hasParity)

			if err := rs.ReconstructData(split); err == nil {
				fmt.Println("Could reconstruct completely")
				for i, data := range split[:k] {
					start := i * len(data)
					copy(m.mapped[start:], data)
				}
				lastValid = m.size - 1
				missing = 0
			} else {
				fmt.Println("Could NOT reconstruct:", err)
			}
		}
	}
	if lastValid == 0 {
		return errors.New("no valid data found")
	}
	if missing > 0 {
		fmt.Println(missing, "bytes missing. Truncating", len(m.filled)-lastValid-1, "from end.")
		out += ".incomplete"
	} else {
		fmt.Println("No bytes missing.")
		out += ".complete"
	}
	m.mapped = m.mapped[:lastValid+1]
	err = os.WriteFile(out, m.mapped, os.ModePerm)
	if err != nil {
		return err
	}
	fmt.Println("Wrote output to", out)
	return nil
}

func combineCrossVer(all map[string][]string, baseName string) error {
	names := make([][]string, 0)
	/// part, verID, file
	files := make([]map[string][]string, 0)
	partNums := make(map[int]int)
	for k, v := range all {
		for _, file := range v {
			part := getPartNum(file)
			partIdx, ok := partNums[part]
			if !ok {
				partIdx = len(names)
				partNums[part] = partIdx
				names = append(names, nil)
				files = append(files, make(map[string][]string))
			}
			names[partIdx] = append(names[partIdx], k)
			files[partIdx][k] = append(files[partIdx][k], file)
		}
	}
	if len(files) == 0 {
		return nil
	}
	for part, partIdx := range partNums {
		if len(files[partIdx]) == 0 {
			continue
		}
		var wantMD5 string
		exportedSizes := make(map[int]bool)
		// block -> data
		combineSharedBlocks := make(map[int][]byte)
		combineFilledBlocks := make(map[int][]byte)
	nextFile:
		for key, file := range files[partIdx] {
			fmt.Println("Reading base version", file[0], "part", part)
			var combined []byte
			var missingAll int
			var lastValidAll int

			attempt := 0
			for block := 0; ; block++ {
				combineFilled := combineFilledBlocks[block]
				combineShared := combineSharedBlocks[block]
			nextAttempt:
				fmt.Printf("Block %d, Base version %q. Part %d. Files %d\n", block+1, key, part, len(file))
				m, err := readAndMap(file, part, block)
				if err != nil {
					return err
				}
				if exportedSizes[m.objSize] {
					fmt.Println("Skipping version", key, "as it has already been exported.")
					continue nextFile
				}
				addedFiles := 0
			compareFile:
				for otherKey, other := range files[partIdx] {
					addedFiles++
					if attempt > 0 && len(m.filled) == len(combineFilled) {
						fmt.Println("Merging previous global data")
						filled := 0
						missing := 0
						for i, v := range combineFilled {
							if v == 1 {
								m.filled[i] = 1
								m.mapped[i] = combineShared[i]
								filled++
							} else {
								missing++
							}
						}
						fmt.Println("Missing", missing, "bytes. Filled", filled, "bytes.")
						break
					}
					if key == otherKey {
						continue
					}

					otherPart := getPartNum(other[0])
					if part != otherPart {
						fmt.Println("part ", part, " != other part", otherPart, other[0])
						continue
					}
					// fmt.Println("part ", part, "other part", otherPart, other[0])
					fmt.Printf("Reading version %q Part %d.\n", otherKey, otherPart)
					// os.Exit(0)
					otherM, err := readAndMap(other, part, block)
					if err != nil {
						fmt.Println(err)
						continue
					}
					if m.objSize != otherM.objSize {
						continue
					}

					// If data+parity matches, combine.
					if m.parity == otherM.parity && m.data == otherM.data {
						for k, v := range m.parityData {
							if otherM.parityData[k] == nil {
								continue
							}
							for i, data := range v {
								if data != nil || otherM.parityData[k][i] == nil {
									continue
								}
								m.parityData[k][i] = otherM.parityData[k][i]
							}
						}
					}

					var ok int
					for i, filled := range otherM.filled[:m.size] {
						if filled == 1 && m.filled[i] == 1 {
							if m.mapped[i] != otherM.mapped[i] {
								fmt.Println("Data mismatch at byte", i, "-  Disregarding version", otherKey)
								continue compareFile
							}
							ok++
						}
					}

					fmt.Printf("Data overlaps (%d bytes). Combining with %q.\n", ok, otherKey)
					for i := range otherM.filled {
						if otherM.filled[i] == 1 {
							m.filled[i] = 1
							m.mapped[i] = otherM.mapped[i]
						}
					}
				}

				lastValid := 0
				missing := 0
				for i := range m.filled {
					if m.filled[i] == 1 {
						lastValid = i
					} else {
						missing++
					}
				}
				if missing == 0 && len(m.parityData) == 1 {
					k := 0
					var parityData map[int][]byte
					for par, pdata := range m.parityData {
						k = par
						parityData = pdata
					}
					if k > 0 {
						rs, err := reedsolomon.New(k, m.shards-k)
						if err != nil {
							return err
						}

						splitData, err := rs.Split(m.mapped)
						if err != nil {
							return err
						}
						// Do separate encode, verify we get the same result.
						err = rs.Encode(splitData)
						if err != nil {
							return err
						}
						misMatches := 0
						for idx, sh := range parityData {
							calculated := splitData[idx]
							if !bytes.Equal(calculated, sh) {
								off := 0
								for i, v := range sh {
									if v != calculated[i] {
										off = i
										break
									}
								}
								calculated := calculated[off:]
								inFile := sh[off:]
								extra := ""
								if len(calculated) != len(inFile) {
									fmt.Println("SIZE MISMATCH", len(calculated), len(inFile))
								} else if len(calculated) > 10 {
									calculated = calculated[:10]
									inFile = inFile[:10]
									extra = "..."
								}
								a := hex.EncodeToString(calculated) + extra
								b := hex.EncodeToString(inFile) + extra
								fmt.Println("MISMATCH in parity shard", idx+1, "at offset", off, "calculated:", a, "found:", b)
								misMatches++
							}
						}
						if misMatches == 0 {
							fmt.Println(m.shards-k, "erasure code shards verified.")
						}
					}
				}
				if missing > 0 && len(m.parityData) > 0 {
					fmt.Println("Attempting to reconstruct using parity sets:")
					for k, v := range m.parityData {
						if missing == 0 {
							break
						}
						fmt.Println("* Setup: Data shards:", k, "- Parity blocks:", m.shards-k)
						rs, err := reedsolomon.New(k, m.shards-k)
						if err != nil {
							return err
						}
						splitData, err := rs.Split(m.mapped)
						if err != nil {
							return err
						}
						splitFilled, err := rs.Split(m.filled)
						if err != nil {
							return err
						}
						// Fill padding...
						padding := len(splitFilled[0])*k - len(m.filled)
						for i := 0; i < padding; i++ {
							arr := splitFilled[k-1]
							arr[len(arr)-i-1] = 1
						}

						hasParity := 0
						parityOK := make([]bool, m.shards)
						for idx, sh := range v {
							splitData[idx] = sh
							if idx >= k && len(sh) > 0 {
								parityOK[idx] = true
								hasParity++
								for i := range splitFilled[idx] {
									splitFilled[idx][i] = 1
								}
							}
						}

						splitDataShards := make([]byte, len(splitFilled[0]))
						for _, sh := range splitFilled {
							for i, v := range sh {
								splitDataShards[i] += v
							}
						}
						var hist [256]int
						for _, v := range splitDataShards {
							hist[v]++
						}

						for _, v := range hist[m.data-hasParity : m.shards] {
							if attempt > 0 {
								break
							}
							if v == 0 {
								continue
							}
							for i, v := range hist[:m.shards] {
								if v > 0 {
									if i < m.data {
										fmt.Println("- Shards:", i, "of", m.data, "Bytes:", v, "Missing: ", v*(m.data-i+hasParity))
									} else {
										fmt.Println("+ Shards:", i, "of", m.data, "Bytes:", v, "Recovering: ", v*(m.data-i+hasParity))
									}
								}
							}
							fmt.Println("Attempting to reconstruct with partial shards")
							offset := 0
							startOffset := 0
							shardConfig := make([]byte, k)
							reconstructAbleConfig := false
							shards := make([][]byte, m.shards)
							for i := range shards {
								shards[i] = make([]byte, 0, len(splitData[0]))
							}
							for offset < len(splitDataShards) {
								newConfig := false
								for shardIdx, shard := range splitFilled[:k] {
									if shardConfig[shardIdx] != shard[offset] {
										newConfig = true
										break
									}
								}
								if newConfig {
									if offset > startOffset && reconstructAbleConfig {
										reconPartial(shards, k, parityOK, splitData, startOffset, offset, rs, shardConfig, splitFilled)
									}
									// Update to new config and add current
									valid := 0
									for shardIdx, shard := range splitFilled[:k] {
										shardConfig[shardIdx] = shard[offset]
										valid += int(shard[offset])
										if shard[offset] == 0 {
											shards[shardIdx] = shards[shardIdx][:0]
										} else {
											shards[shardIdx] = append(shards[shardIdx][:0], splitData[shardIdx][offset])
										}
									}
									reconstructAbleConfig = valid >= m.data-hasParity && valid < m.data
									startOffset = offset
									offset++
									continue
								}
								for shardIdx, ok := range shardConfig {
									if ok != 0 {
										shards[shardIdx] = append(shards[shardIdx], splitData[shardIdx][offset])
									}
								}
								offset++
							}
							if offset > startOffset && reconstructAbleConfig {
								reconPartial(shards, k, parityOK, splitData, startOffset, offset, rs, shardConfig, splitFilled)
							}

							var buf bytes.Buffer
							if err := rs.Join(&buf, splitFilled, m.size); err == nil {
								m.filled = buf.Bytes()
							}
							buf = bytes.Buffer{}
							if err := rs.Join(&buf, splitData, m.size); err == nil {
								m.mapped = buf.Bytes()
							}
							for i, v := range m.filled {
								if v == 0 {
									m.mapped[i] = 0
								}
							}
							break
						}
						ok := k
						for i, sh := range splitFilled {
							for j, v := range sh {
								if v == 0 {
									splitData[i] = nil
									if i < k {
										fmt.Println("Shard", i, "is missing data from offset", i*len(sh)+j)
										ok--
									}
									break
								}
							}
						}

						missing = 0
						lastValid = 0
						for i := range m.filled {
							if m.filled[i] == 1 {
								lastValid = i
							} else {
								missing++
							}
						}
						fmt.Printf("Have %d complete remapped data shards and %d complete parity shards (%d bytes missing). ", ok, hasParity, missing)

						if err := rs.ReconstructData(splitData); err == nil {
							fmt.Println("Could reconstruct completely.")
							for i, data := range splitData[:k] {
								start := i * len(data)
								copy(m.mapped[start:], data)
							}
							lastValid = m.size - 1
							missing = 0
							attempt = 2
							wantMD5 = m.wantMD5
						} else {
							fmt.Println("Could NOT reconstruct:", err, " - Need", m.data, "shards.")
							if attempt == 0 {
								if len(combineShared) == 0 {
									combineShared = make([]byte, len(m.mapped))
									combineFilled = make([]byte, len(m.filled))
								}
								for i := range m.filled {
									if m.filled[i] == 1 && combineFilled[i] == 0 {
										combineShared[i] = m.mapped[i]
										combineFilled[i] = 1
									}
								}
								combineFilledBlocks[block] = combineFilled
								combineSharedBlocks[block] = combineShared
								fmt.Println("Retrying with merged data")
								if addedFiles >= len(files[partIdx]) {
									attempt++
									goto nextAttempt
								}
							}
						}
					}
				}
				if m.blockOffset != len(combined) {
					return fmt.Errorf("Block offset mismatch. Expected %d got %d", m.blockOffset, len(combined))
				}
				combined = append(combined, m.mapped[:m.size]...)
				missingAll += missing
				if lastValid > 0 {
					lastValidAll = lastValid + m.blockOffset
				}
				if m.blocks == block {
					if len(combined) != m.partSize {
						fmt.Println("Combined size mismatch. Expected", m.partSize, "got", len(combined))
					}
					fmt.Println("Reached block", block+1, "of", m.blocks+1, "for", key, "Done.")
					break
				}
			}
			if lastValidAll == 0 {
				return errors.New("no valid data found")
			}
			out := fmt.Sprintf("%s-%s.%05d", key, baseName, part)
			if len(files) == 1 {
				out = fmt.Sprintf("%s-%s", key, baseName)
			}
			if missingAll > 0 {
				out += ".incomplete"
				fmt.Println(missingAll, "bytes missing.")
			} else {
				if wantMD5 != "" {
					sum := md5.Sum(combined)
					gotMD5 := hex.EncodeToString(sum[:])
					if gotMD5 != wantMD5 {
						fmt.Println("MD5 mismatch. Expected", wantMD5, "got", gotMD5)
						out += ".mismatch"
					} else {
						fmt.Println("MD5 verified.")
						out = fmt.Sprintf("verified/%s", baseName)
					}
				} else {
					out = fmt.Sprintf("complete/%s.%05d", baseName, part)
					fmt.Println("No bytes missing.")
				}
			}
			if missingAll == 0 {
				exportedSizes[len(combined)] = true
			}
			err := os.MkdirAll(filepath.Dir(out), os.ModePerm)
			if err != nil {
				return err
			}
			err = os.WriteFile(out, combined, os.ModePerm)
			if err != nil {
				return err
			}
			fmt.Println("Wrote output to", out)
		}
	}
	return nil
}

func reconPartial(shards [][]byte, k int, parityOK []bool, splitData [][]byte, startOffset int, offset int, rs reedsolomon.Encoder, shardConfig []byte, splitFilled [][]byte) {
	// Add parity
	for i := range shards[k:] {
		shards[i+k] = nil
		if parityOK[i+k] {
			shards[i+k] = splitData[i+k][startOffset:offset]
		}
	}
	// Reconstruct with current config.
	if err := rs.ReconstructData(shards); err != nil {
		panic(fmt.Sprintln("Internal error, could NOT partially reconstruct:", err))
	}
	// Copy reconstructed data back.
	verified := 0
	reconstructed := 0
	for shardsIdx, ok := range shardConfig {
		if ok == 0 {
			copy(splitData[shardsIdx][startOffset:], shards[shardsIdx])
			for i := range shards[shardsIdx] {
				if splitFilled[shardsIdx][startOffset+i] == 1 {
					fmt.Println("Internal error: Found filled data at", startOffset+i)
				}
				splitFilled[shardsIdx][startOffset+i] = 1
			}
			reconstructed += len(shards[shardsIdx])
		} else {
			for i := range shards[shardsIdx] {
				if splitFilled[shardsIdx][startOffset+i] == 0 {
					fmt.Println("Internal error: Expected filled data at", startOffset+i)
				}
				if splitData[shardsIdx][startOffset+i] != shards[shardsIdx][i] {
					fmt.Println("Internal error: Mismatch at", startOffset+i)
				}
				verified++
			}
		}
	}
	fmt.Println("Reconstructed", reconstructed, "bytes and verified", verified, "bytes of partial shard with config", shardConfig)
}

// bitrot returns a shard beginning at startOffset after doing bitrot checks.
func bitrot(val []byte, startOffset, shardSize int) ([]byte, error) {
	var res []byte
	for len(val) >= 32 {
		want := val[:32]
		data := val[32:]
		if len(data) > shardSize {
			data = data[:shardSize]
		}

		const magicHighwayHash256Key = "\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0"

		hh, _ := highwayhash.New([]byte(magicHighwayHash256Key))
		hh.Write(data)
		if !bytes.Equal(want, hh.Sum(nil)) {
			return res, fmt.Errorf("bitrot detected")
		}
		res = append(res, data...)
		val = val[32+len(data):]
		if len(res) > startOffset {
			return res[startOffset:], nil
		}
	}
	return res, fmt.Errorf("bitrot: data too short to get block. len(res)=%d, startOffset=%d", len(res), startOffset)
}

// shardSize returns the shard size for a given block size and data blocks.
func shardSize(blockSize, dataBlocks int) (sz int) {
	if dataBlocks == 0 {
		// do nothing on invalid input
		return
	}
	// Make denominator positive
	if dataBlocks < 0 {
		blockSize = -blockSize
		dataBlocks = -dataBlocks
	}
	sz = blockSize / dataBlocks
	if blockSize > 0 && blockSize%dataBlocks != 0 {
		sz++
	}
	return
}

var rePartNum = regexp.MustCompile("/part\\.([0-9]+)/")

func getPartNum(s string) int {
	if m := rePartNum.FindStringSubmatch(s); len(m) > 1 {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	return 1
}
