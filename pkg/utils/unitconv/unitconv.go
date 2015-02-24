/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package unitconv

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Units of various bytes in ascending order
const (
	UNIT_BYTE = 1 << (10 * iota)
	UNIT_KILOBYTE
	UNIT_MEGABYTE
	UNIT_GIGABYTE
	UNIT_TERABYTE
	UNIT_PETABYTE
)

// Convert bytes length in integer to human readable string
func BytesToString(bytes uint64) string {
	var unit string = "B"
	var value uint64 = 0

	switch {
	case bytes >= UNIT_TERABYTE:
		unit = "TB"
		value = uint64(bytes / UNIT_TERABYTE)
	case bytes >= UNIT_GIGABYTE:
		unit = "GB"
		value = uint64(bytes / UNIT_GIGABYTE)
	case bytes >= UNIT_MEGABYTE:
		unit = "MB"
		value = uint64(bytes / UNIT_MEGABYTE)
	case bytes >= UNIT_KILOBYTE:
		unit = "KB"
		value = uint64(bytes / UNIT_KILOBYTE)
	case bytes < UNIT_KILOBYTE && bytes >= UNIT_BYTE:
		unit = "B"
		value = uint64(bytes / UNIT_BYTE)
	}

	return fmt.Sprintf("%d%s", value, unit)
}

// Convert human readable string to bytes length in integer
func StringToBytes(s string) (uint64, error) {
	var bytes uint64
	var err error

	bytes, err = strconv.ParseUint(s, 10, 64)
	if err == nil {
		return bytes, nil
	}

	stringPattern, err := regexp.Compile(`(?i)^(-?\d+)([BKMGT])B?$`)
	if err != nil {
		return 0, err
	}

	parts := stringPattern.FindStringSubmatch(strings.TrimSpace(s))
	if len(parts) < 2 {
		return 0, errors.New("Incorrect string format must be K,KB,M,MB,G,GB")
	}

	value, err := strconv.ParseUint(parts[1], 10, 0)
	if err != nil || value < 1 {
		return 0, err
	}

	unit := strings.ToUpper(parts[2])
	switch unit {
	case "T":
		bytes = value * UNIT_TERABYTE
	case "G":
		bytes = value * UNIT_GIGABYTE
	case "M":
		bytes = value * UNIT_MEGABYTE
	case "K":
		bytes = value * UNIT_KILOBYTE
	case "B":
		bytes = value * UNIT_BYTE
	default:
		return 0, errors.New("Incorrect string format must be K,KB,M,MB,G,GB")
	}

	return bytes, nil
}
