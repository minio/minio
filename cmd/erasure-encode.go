/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"context"
	"io"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// Writes in parallel to writers
type parallelWriter struct {
	writers     []io.Writer
	writeQuorum int
}

// Write writes data to writers in parallel.
func (p *parallelWriter) Write(ctx context.Context, blocks [][]byte) error {
	g := errgroup.New(errgroup.Opts{Total: len(p.writers), FailFactor: 20, Quorum: p.writeQuorum})
	for i := range p.writers {
		i := i
		g.Go(func() error {
			if p.writers[i] == nil {
				return errDiskNotFound
			}

			_, err := p.writers[i].Write(blocks[i])
			return err
		}, i)
	}

	errs := g.Wait()

	// If nilCount >= p.writeQuorum, we return nil. This is because HealFile() uses
	// CreateFile with p.writeQuorum=1 to accommodate healing of single disk.
	// i.e if we do no return here in such a case, reduceWriteQuorumErrs() would
	// return a quorum error to HealFile().
	nilCount := 0
	for i, err := range errs {
		if err == nil {
			nilCount++
		} else {
			p.writers[i] = nil
		}

	}
	if nilCount >= p.writeQuorum {
		return nil
	}

	return reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, p.writeQuorum)
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
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logger.LogIf(ctx, err)
			return 0, err
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
