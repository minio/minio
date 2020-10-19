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

	"sync"

	md5accel "github.com/liangintel/md5accel"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
)

// Writes in parallel to writers
type parallelWriter struct {
	writers     []io.Writer
	writeQuorum int
	errs        []error
}

// Write writes data to writers in parallel.
func (p *parallelWriter) Write(ctx context.Context, blocks [][]byte) error {
	var wg sync.WaitGroup

	for i := range p.writers {
		if p.writers[i] == nil {
			p.errs[i] = errDiskNotFound
			continue
		}

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, p.errs[i] = p.writers[i].Write(blocks[i])
			if p.errs[i] != nil {
				p.writers[i] = nil
			}
		}(i)
	}
	wg.Wait()

	// If nilCount >= p.writeQuorum, we return nil. This is because HealFile() uses
	// CreateFile with p.writeQuorum=1 to accommodate healing of single disk.
	// i.e if we do no return here in such a case, reduceWriteQuorumErrs() would
	// return a quorum error to HealFile().
	nilCount := 0
	for _, err := range p.errs {
		if err == nil {
			nilCount++
		}
	}
	if nilCount >= p.writeQuorum {
		return nil
	}
	return reduceWriteQuorumErrs(ctx, p.errs, objectOpIgnoredErrs, p.writeQuorum)
}

// Encode reads from the reader, erasure-encodes the data and writes to the writers.
func (e *Erasure) Encode(ctx context.Context, src io.Reader, writers []io.Writer, buf_ori []byte, quorum int) (total int64, err error) {
	writer := &parallelWriter{
		writers:     writers,
		writeQuorum: quorum,
		errs:        make([]error, len(writers)),
	}

	buf := buf_ori[:]           // set to original buf which assumes no HW involved
	eng := -1                   // init md5 HW engine index to -1 which assumes no HW involved
	r, ok := src.(*hash.Reader) // check if it's hash.Reader which contains Md5Hash field
	if ok {
		// try to get the md5 engine
		// 1) >= 0: HW engine is got. Previously r.Md5Hash = md5accel.New()
		// 2) == -1: software engine. Previously r.Md5Hash = md5.New()
		// 3) HW engine will be released in subsequent call to MD5Sum()
		// 4) work flow is changed only when eng >= 0
		eng = md5accel.GetAccelerator(r.Md5Hash)

		// tell HW engine don't copy data from MinIO buffer to HW buffer in Md5Hash.Write()
		// because MinIO will receive data from network and directly put data to HW buffer.
		// see subsequent call to md5accel.Accel_get_next_buff
		if(eng >= 0) {
			md5accel.Set_zero_cpy(r.Md5Hash)
		}
	}

	for {
		if(eng >= 0) {
			// use HW buf pool, so no memory copy happen between MinIO and HW, this can increase ~3% throughput
			buf = md5accel.Accel_get_next_buff(r.Md5Hash)
		}
		var blocks [][]byte
		n, err := io.ReadFull(src, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logger.LogIf(ctx, err)
			return 0, err
		}
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if(eng >= 0) {
			if(eof || (n == 0 && total != 0)) {
				// Not really write data, Set_zero_cpy is called in previous code
				// just tell hw the total length of the object, data itself already in HW buf
				md5accel.Accel_write_data(eng, buf, total+int64(n))
		
				// trigger HW to calculate md5 in a go routing
				// 1) this will be running in parallel with subsequent calls of e.EncodeData and writer.Write
				// 2) HW engine will be released here
				md5accel.MD5Sum(r.Md5Hash)
			}
		}
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
