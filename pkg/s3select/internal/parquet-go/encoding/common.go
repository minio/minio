// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
