/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"hash"
	"io"

	"github.com/minio/minio/cmd/ecc"
	"github.com/minio/minio/cmd/logger"
)

// Implementation to calculate bitrot for the whole file.
type wholeBitrotWriter struct {
	disk      StorageAPI
	volume    string
	filePath  string
	shardSize int64 // This is the shard size of the erasure logic
	hash.Hash       // For bitrot hash
}

func (b *wholeBitrotWriter) Write(p []byte) (int, error) {
	err := b.disk.AppendFile(b.volume, b.filePath, p)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return 0, err
	}
	_, err = b.Hash.Write(p)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return 0, err
	}
	return len(p), nil
}

func (b *wholeBitrotWriter) Close() error {
	return nil
}

// Returns whole-file bitrot writer.
func newWholeBitrotWriter(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm, shardSize int64) io.WriteCloser {
	return &wholeBitrotWriter{disk, volume, filePath, shardSize, algo.New()}
}

// Implementation to verify bitrot for the whole file.
type wholeBitrotReader struct {
	disk       StorageAPI
	rc         ecc.Verifier
	volume     string
	filePath   string
	verifier   *BitrotVerifier // Holds the bit-rot info
	tillOffset int64           // Affects the length of data requested in disk.ReadFile depending on Read()'s offset
}

func (b *wholeBitrotReader) ReadAt(buf []byte, offset int64) (n int, err error) {
	if b.rc == nil {
		if b.rc, err = b.disk.ReadFile(b.volume, b.filePath, offset, b.tillOffset-offset, b.verifier); err != nil {
			return 0, err
		}
	}
	return io.ReadFull(b.rc, buf)
}

func (b *wholeBitrotReader) Close() error {
	if b.rc != nil {
		return b.rc.Close()
	}
	return nil
}

// Returns whole-file bitrot reader.
func newWholeBitrotReader(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm, tillOffset int64, sum []byte, shardSize int64) *wholeBitrotReader {
	return &wholeBitrotReader{
		disk:       disk,
		volume:     volume,
		filePath:   filePath,
		verifier:   NewBitrotVerifier(algo, sum, shardSize),
		tillOffset: tillOffset,
	}
}
