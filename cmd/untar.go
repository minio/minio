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

package cmd

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/bzip2"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	"github.com/pierrec/lz4"
)

func detect(r *bufio.Reader) format {
	z, err := r.Peek(4)
	if err != nil {
		return formatUnknown
	}
	for _, f := range magicHeaders {
		if bytes.Equal(f.header, z[:len(f.header)]) {
			return f.f
		}
	}
	return formatUnknown
}

//go:generate stringer -type=format -trimprefix=format $GOFILE
type format int

const (
	formatUnknown format = iota
	formatGzip
	formatZstd
	formatLZ4
	formatS2
	formatBZ2
)

var magicHeaders = []struct {
	header []byte
	f      format
}{
	{
		header: []byte{0x1f, 0x8b, 8},
		f:      formatGzip,
	},
	{
		// Zstd default header.
		header: []byte{0x28, 0xb5, 0x2f, 0xfd},
		f:      formatZstd,
	},
	{
		// Zstd skippable frame header.
		header: []byte{0x2a, 0x4d, 0x18},
		f:      formatZstd,
	},
	{
		// LZ4
		header: []byte{0x4, 0x22, 0x4d, 0x18},
		f:      formatLZ4,
	},
	{
		// Snappy/S2 stream
		header: []byte{0xff, 0x06, 0x00, 0x00},
		f:      formatS2,
	},
	{
		header: []byte{0x42, 0x5a, 'h'},
		f:      formatBZ2,
	},
}

func untar(r io.Reader, putObject func(reader io.Reader, info os.FileInfo, name string)) error {
	bf := bufio.NewReader(r)
	switch f := detect(bf); f {
	case formatGzip:
		gz, err := gzip.NewReader(bf)
		if err != nil {
			return err
		}
		defer gz.Close()
		r = gz
	case formatS2:
		r = s2.NewReader(bf)
	case formatZstd:
		dec, err := zstd.NewReader(bf)
		if err != nil {
			return err
		}
		defer dec.Close()
		r = dec
	case formatBZ2:
		r = bzip2.NewReader(bf)
	case formatLZ4:
		r = lz4.NewReader(bf)
	case formatUnknown:
		r = bf
	default:
		return fmt.Errorf("Unsupported format %s", f)
	}
	tarReader := tar.NewReader(r)
	for {
		header, err := tarReader.Next()

		switch {

		// if no more files are found return
		case err == io.EOF:
			return nil

		// return any other error
		case err != nil:
			return err

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}

		name := header.Name
		if name == slashSeparator {
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir: // = directory
			putObject(tarReader, header.FileInfo(), trimLeadingSlash(pathJoin(name, slashSeparator)))
		case tar.TypeReg, tar.TypeChar, tar.TypeBlock, tar.TypeFifo, tar.TypeGNUSparse: // = regular
			putObject(tarReader, header.FileInfo(), trimLeadingSlash(path.Clean(name)))
		default:
			// ignore symlink'ed
			continue
		}
	}
}
