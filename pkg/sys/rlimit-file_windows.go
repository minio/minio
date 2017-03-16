// +build windows

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package sys

// GetMaxOpenFileLimit - returns maximum file descriptor number that can be opened by this process.
func GetMaxOpenFileLimit() (curLimit, maxLimit uint64, err error) {
	// Nothing to do for windows.
	return curLimit, maxLimit, err
}

// SetMaxOpenFileLimit - sets maximum file descriptor number that can be opened by this process.
func SetMaxOpenFileLimit(curLimit, maxLimit uint64) error {
	// Nothing to do for windows.
	return nil
}
