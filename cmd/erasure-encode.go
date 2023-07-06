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
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/logger"
)

// Writes in parallel to writers
type parallelWriter struct {
	writers     []io.Writer
	writeQuorum int
	healing     bool
}

// Write writes data to writers in parallel.
func (p *parallelWriter) Write(ctx context.Context, blocks [][]byte) error {
	var wg sync.WaitGroup

	doneCh := make(chan int, len(p.writers))
	defer close(doneCh)

	if !p.healing {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var ints []int
			for {
				select {
				case i := <-doneCh:
					ints = append(ints, i)
				}

				// We have enough success >= p.writeQuorum, we have success.
				if len(ints) >= p.writeQuorum {
					for i := range p.writers {
						var success bool
						for _, v := range ints {
							if v == i {
								success = true
								break
							}
						}
						if success {
							continue
						}
						if p.writers[i] != nil {
							bw, ok := p.writers[i].(*streamingBitrotWriter)
							if ok {
								bw.CloseSlow()
							}
						}
					}
					return
				}
			}
		}()
	}

	for i := range p.writers {
		if p.writers[i] == nil {
			continue
		}
		bw, ok := p.writers[i].(*streamingBitrotWriter)
		if ok && bw.IsClosed() {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n, err := p.writers[i].Write(blocks[i])
			if err == nil && n != len(blocks[i]) {
				err = io.ErrShortWrite
			}
			if err != nil {
				bw, ok := p.writers[i].(*streamingBitrotWriter)
				if ok {
					bw.CloseSlow()
				}
				return
			}
			if !p.healing {
				select {
				case doneCh <- i:
				default:
				}
			}
		}(i)
	}
	wg.Wait()

	errs := make([]error, len(p.writers))
	for i := range p.writers {
		if p.writers[i] == nil {
			errs[i] = errDiskNotFound
		}
		bw, ok := p.writers[i].(*streamingBitrotWriter)
		if ok && bw.IsClosed() {
			errs[i] = errDiskNotFound
		}
	}

	// If nilCount >= p.writeQuorum, we return nil. This is because HealFile() uses
	// CreateFile with p.writeQuorum=1 to accommodate healing of single disk.
	// i.e if we do no return here in such a case, reduceWriteQuorumErrs() would
	// return a quorum error to HealFile().
	nilCount := countErrs(errs, nil)
	if nilCount >= p.writeQuorum {
		return nil
	}

	writeErr := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, p.writeQuorum)
	return fmt.Errorf("%w (offline-disks=%d/%d)", writeErr, countErrs(errs, errDiskNotFound), len(p.writers))
}

// Encode reads from the reader, erasure-encodes the data and writes to the writers.
func (e *Erasure) Encode(ctx context.Context, src io.Reader, writers []io.Writer, buf []byte, quorum int) (total int64, err error) {
	writer := &parallelWriter{
		writers:     writers,
		writeQuorum: quorum,
	}

	for {
		var blocks [][]byte
		n, err := io.ReadFull(src, buf)
		if err != nil {
			if !IsErrIgnored(err, []error{
				io.EOF,
				io.ErrUnexpectedEOF,
			}...) {
				if !hash.IsChecksumMismatch(err) {
					logger.LogIf(ctx, err)
				}
				return 0, err
			}
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if n == 0 && total != 0 {
			// Reached EOF, nothing more to be done.
			break
		}
		// We take care of the situation where if n == 0 and total == 0 by creating empty data and parity files.
		blocks, err = e.EncodeData(ctx, buf[:n])
		if err != nil {
			logger.LogIf(ctx, err)

			return 0, err
		}

		if err = writer.Write(ctx, blocks); err != nil {
			logger.LogIf(ctx, err)
			return 0, err
		}
		total += int64(n)
		if eof {
			break
		}
	}
	return total, nil
}
