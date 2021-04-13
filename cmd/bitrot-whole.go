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
	"fmt"
	"hash"
	"io"

	"github.com/minio/minio/cmd/logger"
)

// Implementation to calculate bitrot for the whole file.
type wholeBitrotWriter struct {
	disk StorageAPI
	hash.Hash
	volume    string
	filePath  string
	shardSize int64
}

func (b *wholeBitrotWriter) Write(p []byte) (int, error) {
	err := b.disk.AppendFile(context.TODO(), b.volume, b.filePath, p)
	if err != nil {
		logger.LogIf(GlobalContext, fmt.Errorf("Disk: %s returned %w", b.disk, err))
		return 0, err
	}
	_, err = b.Hash.Write(p)
	if err != nil {
		logger.LogIf(GlobalContext, fmt.Errorf("Disk: %s returned %w", b.disk, err))
		return 0, err
	}
	return len(p), nil
}

func (b *wholeBitrotWriter) Close() error {
	return nil
}

// Returns whole-file bitrot writer.
func newWholeBitrotWriter(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm, shardSize int64) io.WriteCloser {
	return &wholeBitrotWriter{disk, algo.New(), volume, filePath, shardSize}
}

// Implementation to verify bitrot for the whole file.
type wholeBitrotReader struct {
	disk       StorageAPI
	verifier   *BitrotVerifier
	volume     string
	filePath   string
	buf        []byte
	tillOffset int64
}

func (b *wholeBitrotReader) ReadAt(buf []byte, offset int64) (n int, err error) {
	if b.buf == nil {
		b.buf = make([]byte, b.tillOffset-offset)
		if _, err := b.disk.ReadFile(context.TODO(), b.volume, b.filePath, offset, b.buf, b.verifier); err != nil {
			logger.LogIf(GlobalContext, fmt.Errorf("Disk: %s -> %s/%s returned %w", b.disk, b.volume, b.filePath, err))
			return 0, err
		}
	}
	if len(b.buf) < len(buf) {
		logger.LogIf(GlobalContext, fmt.Errorf("Disk: %s -> %s/%s returned %w", b.disk, b.volume, b.filePath, errLessData))
		return 0, errLessData
	}
	n = copy(buf, b.buf)
	b.buf = b.buf[n:]
	return n, nil
}

// Returns whole-file bitrot reader.
func newWholeBitrotReader(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm, tillOffset int64, sum []byte) *wholeBitrotReader {
	return &wholeBitrotReader{
		disk:       disk,
		volume:     volume,
		filePath:   filePath,
		verifier:   &BitrotVerifier{sum, algo},
		tillOffset: tillOffset,
		buf:        nil,
	}
}
