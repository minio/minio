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

package cmd

// SetupType - enum for setup type.
type SetupType int

const (
	// UnknownSetupType - starts with unknown setup type.
	UnknownSetupType SetupType = iota

	// FSSetupType - FS setup type enum.
	FSSetupType

	// ErasureSDSetupType - Erasure single drive setup enum.
	ErasureSDSetupType

	// ErasureSetupType - Erasure setup type enum.
	ErasureSetupType

	// DistErasureSetupType - Distributed Erasure setup type enum.
	DistErasureSetupType
)

func (setupType SetupType) String() string {
	switch setupType {
	case FSSetupType:
		return globalMinioModeFS
	case ErasureSDSetupType:
		return globalMinioModeErasureSD
	case ErasureSetupType:
		return globalMinioModeErasure
	case DistErasureSetupType:
		return globalMinioModeDistErasure
	}

	return "unknown"
}
