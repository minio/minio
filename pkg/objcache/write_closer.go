/*
 * Minio Cloud Storage, (C) 2021 Minio, Inc.
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
 *
 */

// Package objcache implements in memory caching methods.
package objcache

import "github.com/valyala/bytebufferpool"

// Is an Closer wrapper for bytebufferpool, upon close
// calls the defined onClose function.
type writeCloser struct {
	*bytebufferpool.ByteBuffer
	onClose func() error
}

// On close, onClose() is called which checks if all object contents
// have been written so that it can save the buffer to the cache.
func (c writeCloser) Close() (err error) {
	return c.onClose()
}
