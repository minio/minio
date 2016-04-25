/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"io"
	"sync"
)

// blockingWriteCloser is a WriteCloser that blocks until released.
type blockingWriteCloser struct {
	writer  io.WriteCloser  // Embedded writer.
	release *sync.WaitGroup // Waitgroup for atomicity.
	err     error
}

// Write to the underlying writer.
func (b *blockingWriteCloser) Write(data []byte) (int, error) {
	return b.writer.Write(data)
}

// Close blocks until another goroutine calls Release(error). Returns
// error code if either writer fails or Release is called with an error.
func (b *blockingWriteCloser) Close() error {
	err := b.writer.Close()
	b.release.Wait()
	return err
}

// Release the Close, causing it to unblock. Only call this
// once. Calling it multiple times results in a panic.
func (b *blockingWriteCloser) Release() {
	b.release.Done()
	return
}

// newBlockingWriteCloser Creates a new write closer that must be
// released by the read consumer.
func newBlockingWriteCloser(writer io.WriteCloser) *blockingWriteCloser {
	// Wait group for the go-routine.
	wg := &sync.WaitGroup{}
	// Add to the wait group to wait for.
	wg.Add(1)
	return &blockingWriteCloser{
		writer:  writer,
		release: wg,
	}
}
