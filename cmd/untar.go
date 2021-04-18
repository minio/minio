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
