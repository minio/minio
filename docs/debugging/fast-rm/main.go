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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/cli"
	"github.com/tinylib/msgp/msgp"
)

func isDirEmpty(dirname string) (bool, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return false, err
	}
	names, err := f.Readdirnames(1)
	f.Close()
	if err != nil {
		return false, err
	}
	return len(names) == 0, nil
}

// dir is clean and never has a trailing slash
func recursiveDeleteIfEmpty(dir string) {
	// /mnt/minio1/bucketname/dir
	if strings.Count(dir, "/") < 4 {
		return
	}
	empty, err := isDirEmpty(dir)
	if err != nil {
		return
	}
	if !empty {
		return
	}
	log.Println("removing dir", dir)
	err = os.Remove(dir)
	if err == nil {
		recursiveDeleteIfEmpty(filepath.Dir(dir))
	}
}

func removeObjectAndEmptyParents(filename string, base string) {
	if !strings.HasPrefix(filename, base) {
		return
	}
	os.RemoveAll(filename)
	recursiveDeleteIfEmpty(filepath.Dir(filename))
}

func removeExpiredXLMeta(xlMetaPath, base string) {
	pathToRemove := filepath.Dir(xlMetaPath)
	if verbose {
		log.Println("removing", pathToRemove)
	}
	if !dryRun {
		removeObjectAndEmptyParents(pathToRemove, base)
	}
}

var (
	waitFactor      int
	basePath        string
	dryRun, verbose bool
	olderThan       time.Duration
)

func mainAction(c *cli.Context) error {

	var err error

	waitFactor = c.Int("wait-factor")
	basePath = c.String("base-path")
	if basePath == "" || !strings.HasSuffix(basePath, "/") {
		log.Fatal("--base-path should not be empty and should finish with a trailing slash")
	}
	dryRun = c.Bool("dry-run")
	verbose = c.Bool("verbose")
	olderThan, err = time.ParseDuration(c.String("older-than"))
	if err != nil {
		log.Fatal("Unable to parse --older-than flag:", err)
	}

	refModTime := time.Now().UTC().Add(-olderThan).UnixNano()

	calculateNewestVersion := func(r io.Reader) (int64, error) {
		b, err := io.ReadAll(r)
		if err != nil {
			return 0, err
		}
		b, _, minor, err := checkXL2V1(b)
		if err != nil {
			return 0, err
		}

		if minor == 3 {
			v, b, err := msgp.ReadBytesZC(b)
			if err != nil {
				return 0, err
			}
			if _, nbuf, err := msgp.ReadUint32Bytes(b); err == nil {
				// Read metadata CRC (added in v2, ignore if not found)
				b = nbuf
			}
			var latestModTime int64
			nVers, v, err := decodeXLHeaders(v)
			if err != nil {
				return 0, err
			}
			err = decodeVersions(v, nVers, func(idx int, hdr, meta []byte) error {
				var header xlMetaV2VersionHeaderV2
				if _, err := header.UnmarshalMsg(hdr); err != nil {
					return err
				}
				if header.ModTime > latestModTime {
					latestModTime = header.ModTime
				}
				return nil
			})
			if err != nil {
				return 0, err
			}
			if latestModTime == 0 {
				return 0, errors.New("no modtime found")
			}
			return latestModTime, nil
		} else {
			return 0, fmt.Errorf("ignoring metadata version %d", minor)
		}
	}

	if len(c.Args()) == 0 {
		log.Fatalln("specify at least one disk mount")
	}

	// Validation of all arguments
	for _, arg := range c.Args() {
		if !strings.HasPrefix(arg, basePath) {
			log.Fatal("passed arguments should start with /mnt/")
		}
	}

	var wg sync.WaitGroup
	for _, arg := range c.Args() {
		wg.Add(1)
		go func(arg string) {
			defer wg.Done()
			err := filepath.Walk(arg, func(path string, info fs.FileInfo, err error) error {
				if err != nil {
					return nil
				}
				if info.IsDir() || info.Name() != "xl.meta" {
					return nil
				}

				if strings.Contains(path, ".minio.sys") {
					return nil
				}

				now := time.Now()

				// Get xl.meta disk modtime
				latestModTime := info.ModTime().UnixNano()

				if latestModTime > refModTime {
					// Parse xl.meta content and check if all versions
					// are older than the specified --older-than argument
					f, e := os.Open(path)
					if e != nil {
						return nil
					}
					latestModTime, e = calculateNewestVersion(f)
					f.Close()
					if e != nil {
						return nil
					}
				}

				if latestModTime < refModTime {
					removeExpiredXLMeta(path, arg)
					if waitFactor > 0 {
						// Slow down
						time.Sleep(time.Duration(waitFactor) * time.Since(now))
					}
				}
				return nil
			})
			if err != nil {
				log.Println("ERROR when walking the path %q: %v\n", arg, err)
			}
		}(arg)
	}
	wg.Wait()
	return nil
}

func main() {
	app := cli.NewApp()
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .VisibleFlags}}[FLAGS]{{end}} [DIRS]...

Pass multiple mount points to scan for xl.meta that have all versions
older than to --older-than e.g. 120h (5 days)

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
`

	app.Flags = []cli.Flag{
		cli.IntFlag{
			Usage: "A wait factor",
			Name:  "wait-factor",
		},
		cli.DurationFlag{
			Usage: "remove only xl.meta with all versions older than",
			Name:  "older-than",
			Value: 120 * time.Hour,
		},
		cli.StringFlag{
			Usage: "specify the base path of folders to scan",
			Name:  "base-path",
		},
		cli.BoolFlag{
			Usage: "fake removal",
			Name:  "dry-run",
		},
		cli.BoolFlag{
			Usage: "verbose printing",
			Name:  "verbose",
		},
	}

	app.Action = mainAction

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
