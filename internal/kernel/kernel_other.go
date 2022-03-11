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

//go:build !linux
// +build !linux

package kernel

// VersionFromRelease only implemented on Linux.
func VersionFromRelease(_ string) (uint32, error) {
	return 0, nil
}

// Version only implemented on Linux.
func Version(_, _, _ int) uint32 {
	return 0
}

// CurrentVersion only implemented on Linux.
func CurrentVersion() (uint32, error) {
	return 0, nil
}
