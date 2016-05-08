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

// waitCloser implements a Closer that blocks until released, this
// prevents the writer closing one end of a pipe while making sure
// that all data is written and committed to disk on the end.
// Additionally this also implements Write().
type waitCloser struct {
	wg     *sync.WaitGroup // Waitgroup for atomicity.
	writer io.WriteCloser  // Embedded writer.
}

// Write to the underlying writer.
func (b *waitCloser) Write(data []byte) (int, error) {
	return b.writer.Write(data)
}

// Close blocks until another goroutine calls Release(error). Returns
// error code if either writer fails or Release is called with an error.
func (b *waitCloser) Close() error {
	err := b.writer.Close()
	b.wg.Wait()
	return err
}

// CloseWithError closes the writer; subsequent read to the read
// half of the pipe will return the error err.
func (b *waitCloser) CloseWithError(err error) error {
	w, ok := b.writer.(*io.PipeWriter)
	if ok {
		return w.CloseWithError(err)
	}
	return err
}

// release the Close, causing it to unblock. Only call this
// once. Calling it multiple times results in a panic.
func (b *waitCloser) release() {
	b.wg.Done()
	return
}

// newWaitCloser creates a new write closer that must be
// released by the read consumer.
func newWaitCloser(writer io.WriteCloser) *waitCloser {
	// Wait group for the go-routine.
	wg := &sync.WaitGroup{}
	// Add to the wait group to wait for.
	wg.Add(1)
	return &waitCloser{
		wg:     wg,
		writer: writer,
	}
}
