// Copyright (c) 2015-2024 MinIO, Inc.
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

package hash

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"math/bits"
)

// AddPart will merge a part checksum into the current,
// as if the content of each was appended.
// The size of the content that produced the second checksum must be provided.
// Not all checksum types can be merged, use the CanMerge method to check.
// Checksum types must match.
func (c *Checksum) AddPart(other Checksum, size int64) error {
	if !other.Type.CanMerge() {
		return fmt.Errorf("checksum type cannot be merged")
	}
	if size == 0 {
		return nil
	}
	if !c.Type.Is(other.Type.Base()) {
		return fmt.Errorf("checksum type does not match got %s and %s", c.Type.String(), other.Type.String())
	}
	// If never set, just add first checksum.
	if len(c.Raw) == 0 {
		c.Raw = other.Raw
		c.Encoded = other.Encoded
		return nil
	}
	if !c.Valid() {
		return fmt.Errorf("invalid base checksum")
	}
	if !other.Valid() {
		return fmt.Errorf("invalid part checksum")
	}

	switch c.Type.Base() {
	case ChecksumCRC32:
		v := crc32Combine(crc32.IEEE, binary.BigEndian.Uint32(c.Raw), binary.BigEndian.Uint32(other.Raw), size)
		binary.BigEndian.PutUint32(c.Raw, v)
	case ChecksumCRC32C:
		v := crc32Combine(crc32.Castagnoli, binary.BigEndian.Uint32(c.Raw), binary.BigEndian.Uint32(other.Raw), size)
		binary.BigEndian.PutUint32(c.Raw, v)
	case ChecksumCRC64NVME:
		v := crc64Combine(bits.Reverse64(crc64NVMEPolynomial), binary.BigEndian.Uint64(c.Raw), binary.BigEndian.Uint64(other.Raw), size)
		binary.BigEndian.PutUint64(c.Raw, v)
	default:
		return fmt.Errorf("unknown checksum type: %s", c.Type.String())
	}
	c.Encoded = base64.StdEncoding.EncodeToString(c.Raw)
	return nil
}

const crc64NVMEPolynomial = 0xad93d23594c93659

var crc64Table = crc64.MakeTable(bits.Reverse64(crc64NVMEPolynomial))

// Following is ported from C to Go in 2016 by Justin Ruggles, with minimal alteration.
// Used uint for unsigned long. Used uint32 for input arguments in order to match
// the Go hash/crc32 package. zlib CRC32 combine (https://github.com/madler/zlib)
// Modified for hash/crc64 by Klaus Post, 2024.
func gf2MatrixTimes(mat []uint64, vec uint64) uint64 {
	var sum uint64

	for vec != 0 {
		if vec&1 != 0 {
			sum ^= mat[0]
		}
		vec >>= 1
		mat = mat[1:]
	}
	return sum
}

func gf2MatrixSquare(square, mat []uint64) {
	if len(square) != len(mat) {
		panic("square matrix size mismatch")
	}
	for n := range mat {
		square[n] = gf2MatrixTimes(mat, mat[n])
	}
}

// crc32Combine returns the combined CRC-32 hash value of the two passed CRC-32
// hash values crc1 and crc2. poly represents the generator polynomial
// and len2 specifies the byte length that the crc2 hash covers.
func crc32Combine(poly uint32, crc1, crc2 uint32, len2 int64) uint32 {
	// degenerate case (also disallow negative lengths)
	if len2 <= 0 {
		return crc1
	}

	even := make([]uint64, 32) // even-power-of-two zeros operator
	odd := make([]uint64, 32)  // odd-power-of-two zeros operator

	// put operator for one zero bit in odd
	odd[0] = uint64(poly) // CRC-32 polynomial
	row := uint64(1)
	for n := 1; n < 32; n++ {
		odd[n] = row
		row <<= 1
	}

	// put operator for two zero bits in even
	gf2MatrixSquare(even, odd)

	// put operator for four zero bits in odd
	gf2MatrixSquare(odd, even)

	// apply len2 zeros to crc1 (first square will put the operator for one
	// zero byte, eight zero bits, in even)
	crc1n := uint64(crc1)
	for {
		// apply zeros operator for this bit of len2
		gf2MatrixSquare(even, odd)
		if len2&1 != 0 {
			crc1n = gf2MatrixTimes(even, crc1n)
		}
		len2 >>= 1

		// if no more bits set, then done
		if len2 == 0 {
			break
		}

		// another iteration of the loop with odd and even swapped
		gf2MatrixSquare(odd, even)
		if len2&1 != 0 {
			crc1n = gf2MatrixTimes(odd, crc1n)
		}
		len2 >>= 1

		// if no more bits set, then done
		if len2 == 0 {
			break
		}
	}

	// return combined crc
	crc1n ^= uint64(crc2)
	return uint32(crc1n)
}

func crc64Combine(poly uint64, crc1, crc2 uint64, len2 int64) uint64 {
	// degenerate case (also disallow negative lengths)
	if len2 <= 0 {
		return crc1
	}

	even := make([]uint64, 64) // even-power-of-two zeros operator
	odd := make([]uint64, 64)  // odd-power-of-two zeros operator

	// put operator for one zero bit in odd
	odd[0] = poly // CRC-64 polynomial
	row := uint64(1)
	for n := 1; n < 64; n++ {
		odd[n] = row
		row <<= 1
	}

	// put operator for two zero bits in even
	gf2MatrixSquare(even, odd)

	// put operator for four zero bits in odd
	gf2MatrixSquare(odd, even)

	// apply len2 zeros to crc1 (first square will put the operator for one
	// zero byte, eight zero bits, in even)
	crc1n := crc1
	for {
		// apply zeros operator for this bit of len2
		gf2MatrixSquare(even, odd)
		if len2&1 != 0 {
			crc1n = gf2MatrixTimes(even, crc1n)
		}
		len2 >>= 1

		// if no more bits set, then done
		if len2 == 0 {
			break
		}

		// another iteration of the loop with odd and even swapped
		gf2MatrixSquare(odd, even)
		if len2&1 != 0 {
			crc1n = gf2MatrixTimes(odd, crc1n)
		}
		len2 >>= 1

		// if no more bits set, then done
		if len2 == 0 {
			break
		}
	}

	// return combined crc
	crc1n ^= crc2
	return crc1n
}
