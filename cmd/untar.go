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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/cosnicolaou/pbzip2"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/pierrec/lz4/v4"
)

// Max bzip2 concurrency across calls. 50% of GOMAXPROCS.
var bz2Limiter = pbzip2.CreateConcurrencyPool((runtime.GOMAXPROCS(0) + 1) / 2)

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

type untarOptions struct {
	ignoreDirs bool
	ignoreErrs bool
	prefixAll  string
}

// disconnectReader will ensure that no reads can take place on
// the upstream reader after close has been called.
type disconnectReader struct {
	r  io.Reader
	mu sync.Mutex
}

func (d *disconnectReader) Read(p []byte) (n int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.r != nil {
		return d.r.Read(p)
	}
	return 0, errors.New("reader closed")
}

func (d *disconnectReader) Close() error {
	d.mu.Lock()
	d.r = nil
	d.mu.Unlock()
	return nil
}

func untar(ctx context.Context, r io.Reader, putObject func(reader io.Reader, info os.FileInfo, name string) error, o untarOptions) error {
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
		// Limit to 16 MiB per stream.
		dec, err := zstd.NewReader(bf, zstd.WithDecoderMaxWindow(16<<20))
		if err != nil {
			return err
		}
		defer dec.Close()
		r = dec
	case formatBZ2:
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		r = pbzip2.NewReader(ctx, bf, pbzip2.DecompressionOptions(
			pbzip2.BZConcurrency((runtime.GOMAXPROCS(0)+1)/2),
			pbzip2.BZConcurrencyPool(bz2Limiter)))
	case formatLZ4:
		r = lz4.NewReader(bf)
	case formatUnknown:
		r = bf
	default:
		return fmt.Errorf("Unsupported format %s", f)
	}
	tarReader := tar.NewReader(r)
	n := 0
	asyncWriters := make(chan struct{}, 16)
	var wg sync.WaitGroup

	var asyncErr error
	var asyncErrMu sync.Mutex
	for {
		if !o.ignoreErrs {
			asyncErrMu.Lock()
			err := asyncErr
			asyncErrMu.Unlock()
			if err != nil {
				return err
			}
		}

		header, err := tarReader.Next()
		switch {
		// if no more files are found return
		case err == io.EOF:
			wg.Wait()
			return asyncErr

		// return any other error
		case err != nil:
			wg.Wait()
			extra := ""
			if n > 0 {
				extra = fmt.Sprintf(" after %d successful object(s)", n)
			}
			return fmt.Errorf("tar file error: %w%s", err, extra)

		// if the header is nil, just skip it (not sure how this happens)
		case header == nil:
			continue
		}

		name := header.Name
		switch path.Clean(name) {
		case ".", slashSeparator:
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir: // = directory
			if o.ignoreDirs {
				continue
			}
			name = trimLeadingSlash(pathJoin(name, slashSeparator))
		case tar.TypeReg, tar.TypeChar, tar.TypeBlock, tar.TypeFifo, tar.TypeGNUSparse: // = regular
			name = trimLeadingSlash(path.Clean(name))
		default:
			// ignore symlink'ed
			continue
		}
		if o.prefixAll != "" {
			name = pathJoin(o.prefixAll, name)
		}

		// Do small files async
		n++
		if header.Size <= xioutil.MediumBlock {
			asyncWriters <- struct{}{}
			bufp := xioutil.ODirectPoolMedium.Get()
			b := (*bufp)[:header.Size]
			if _, err := io.ReadFull(tarReader, b); err != nil {
				return err
			}
			wg.Add(1)
			go func(name string, fi fs.FileInfo, b []byte) {
				rc := disconnectReader{r: bytes.NewReader(b)}
				defer func() {
					rc.Close()
					<-asyncWriters
					wg.Done()
					xioutil.ODirectPoolMedium.Put(bufp)
				}()
				if err := putObject(&rc, fi, name); err != nil {
					if o.ignoreErrs {
						s3LogIf(ctx, err)
						return
					}
					asyncErrMu.Lock()
					if asyncErr == nil {
						asyncErr = err
					}
					asyncErrMu.Unlock()
				}
			}(name, header.FileInfo(), b)
			continue
		}

		// If zero or earlier modtime, set to current.
		// Otherwise the resulting objects will be invalid.
		if header.ModTime.UnixNano() <= 0 {
			header.ModTime = time.Now()
		}

		// Sync upload.
		rc := disconnectReader{r: tarReader}
		if err := putObject(&rc, header.FileInfo(), name); err != nil {
			rc.Close()
			if o.ignoreErrs {
				s3LogIf(ctx, err)
				continue
			}
			return err
		}
		rc.Close()
	}
}
