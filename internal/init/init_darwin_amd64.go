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

package init

import (
	"os"

	"github.com/klauspost/cpuid/v2"
)

func init() {
	// All MinIO operations must be under UTC.
	os.Setenv("TZ", "UTC")

	// Temporary workaround for
	// https://github.com/golang/go/issues/49233
	// Keep until upstream has been fixed.
	cpuid.CPU.Disable(cpuid.AVX512F, cpuid.AVX512BW, cpuid.AVX512CD, cpuid.AVX512DQ,
		cpuid.AVX512ER, cpuid.AVX512FP16, cpuid.AVX512IFMA, cpuid.AVX512PF, cpuid.AVX512VBMI,
		cpuid.AVX512VBMI2, cpuid.AVX512VL, cpuid.AVX512VNNI, cpuid.AVX512VP2INTERSECT, cpuid.AVX512VPOPCNTDQ)
}
