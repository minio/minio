/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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

package minio

import (
	"io/ioutil"
	"os"
	"sync"
)

// tempFile - temporary file container.
type tempFile struct {
	*os.File
	mutex *sync.Mutex
}

// newTempFile returns a new temporary file, once closed it automatically deletes itself.
func newTempFile(prefix string) (*tempFile, error) {
	// use platform specific temp directory.
	file, err := ioutil.TempFile(os.TempDir(), prefix)
	if err != nil {
		return nil, err
	}
	return &tempFile{
		File:  file,
		mutex: &sync.Mutex{},
	}, nil
}

// Close - closer wrapper to close and remove temporary file.
func (t *tempFile) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.File != nil {
		// Close the file.
		if err := t.File.Close(); err != nil {
			return err
		}
		// Remove file.
		if err := os.Remove(t.File.Name()); err != nil {
			return err
		}
		t.File = nil
	}
	return nil
}
