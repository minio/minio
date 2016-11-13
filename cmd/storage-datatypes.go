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

package cmd

import (
	"os"
	"time"
)

// VolInfo - represents volume stat information.
type VolInfo struct {
	// Name of the volume.
	Name string

	// Date and time when the volume was created.
	Created time.Time
}

// FileInfo - represents file stat information.
type FileInfo struct {
	// Name of the volume.
	Volume string

	// Name of the file.
	Name string

	// Date and time when the file was last modified.
	ModTime time.Time

	// Total file size.
	Size int64

	// File mode bits.
	Mode os.FileMode
}
