/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package encoding

import (
	"github.com/minio/minio/pkg/s3select/internal/parquet-go/common"
)

// Refer https://en.wikipedia.org/wiki/LEB128#Unsigned_LEB128
func varIntEncode(ui64 uint64) []byte {
	if ui64 == 0 {
		return []byte{0}
	}

	length := int(common.BitWidth(ui64)+6) / 7
	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = byte(ui64&0x7F) | 0x80
		ui64 >>= 7
	}
	data[length-1] &= 0x7F

	return data
}
