// Copyright (c) 2015-2022 MinIO, Inc.
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

//go:build (linux && 386) || (linux && amd64) || (linux && arm64) || (linux && mips64) || (linux && mips)
// +build linux,386 linux,amd64 linux,arm64 linux,mips64 linux,mips

package kernel

func utsnameStr(in []int8) string {
	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		if in[i] == 0x00 {
			break
		}
		out = append(out, byte(in[i]))
	}
	return string(out)
}
