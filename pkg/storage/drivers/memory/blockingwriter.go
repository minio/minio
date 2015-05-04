/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package memory

import (
	"io"
	"sync"
)

// BlockingWriteCloser is a WriteCloser that blocks until released
type BlockingWriteCloser struct {
	w       io.WriteCloser
	release *sync.WaitGroup
	err     error
}

// Write to the underlying writer
func (b *BlockingWriteCloser) Write(p []byte) (int, error) {
	n, err := b.w.Write(p)
	if err != nil {
		b.err = err
	}
	return n, b.err
}

// Close blocks until another goroutine calls Release(error). Returns error code if either
// writer fails or Release is called with an error.
func (b *BlockingWriteCloser) Close() error {
	err := b.w.Close()
	if err != nil {
		b.err = err
	}
	b.release.Wait()
	return b.err
}

// Release the Close, causing it to unblock. Only call this once. Calling it multiple times results in a panic.
func (b *BlockingWriteCloser) Release(err error) {
	b.release.Done()
	if err != nil {
		b.err = err
	}
	return
}

// NewBlockingWriteCloser Creates a new write closer that must be released by the read consumer.
func NewBlockingWriteCloser(w io.WriteCloser) *BlockingWriteCloser {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return &BlockingWriteCloser{w: w, release: wg}
}
