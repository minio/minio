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
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// IEC Sizes, kibis of bits
const (
	Byte = 1 << (iota * 10)
	KiByte
	MiByte
	GiByte
	TiByte
	PiByte
	EiByte
)

// Scientific notation Sizes.
const (
	IByte = 1
	KByte = IByte * 1000
	MByte = KByte * 1000
	GByte = MByte * 1000
	TByte = GByte * 1000
	PByte = TByte * 1000
	EByte = PByte * 1000
)

// This table represents both IEC and SI notations with their corresponding values.
var bytesSizeTable = map[string]uint64{
	"b":   Byte,
	"kib": KiByte,
	"kb":  KByte,
	"mib": MiByte,
	"mb":  MByte,
	"gib": GiByte,
	"gb":  GByte,
	"tib": TiByte,
	"tb":  TByte,
	"pib": PiByte,
	"pb":  PByte,
	"eib": EiByte,
	"eb":  EByte,
	// Without suffix
	"":   Byte,
	"ki": KiByte,
	"k":  KByte,
	"mi": MiByte,
	"m":  MByte,
	"gi": GiByte,
	"g":  GByte,
	"ti": TiByte,
	"t":  TByte,
	"pi": PiByte,
	"p":  PByte,
	"ei": EiByte,
	"e":  EByte,
}

// strconvBytes parses a string representation of bytes into the number
// of bytes it represents.
//
// See Also: Bytes, IBytes.
//
// ParseBytes("42MB") -> 42000000, nil
// ParseBytes("42mib") -> 44040192, nil
func strconvBytes(s string) (uint64, error) {
	lastDigit := 0
	// Calculates the final integer value.
	for _, r := range s {
		// This supports decimals as well.
		if !(unicode.IsDigit(r) || r == '.') {
			break
		}
		lastDigit++
	}

	// Float parsing to deal with decimal inputs.
	f, err := strconv.ParseFloat(s[:lastDigit], 64)
	if err != nil {
		return 0, err
	}

	// Fetch the corresponding byte size for notation.
	byteSize := strings.ToLower(strings.TrimSpace(s[lastDigit:]))
	size, ok := bytesSizeTable[byteSize]
	if !ok {
		return 0, fmt.Errorf("Unrecognized size notation name: %v", byteSize)
	}
	f *= float64(size)
	// Return an error if final value overflows uint64 max.
	if f >= math.MaxUint64 {
		return 0, fmt.Errorf("too large: %v", s)
	}
	// Success.
	return uint64(f), nil
}
