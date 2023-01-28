// Copyright (c) 2015-2023 MinIO, Inc.
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

package event

import (
	"fmt"
)

// ErrUnknownRegion - unknown region error.
type ErrUnknownRegion struct {
	Region string
}

func (err ErrUnknownRegion) Error() string {
	return fmt.Sprintf("unknown region '%v'", err.Region)
}

// ErrARNNotFound - ARN not found error.
type ErrARNNotFound struct {
	ARN ARN
}

func (err ErrARNNotFound) Error() string {
	return fmt.Sprintf("ARN '%v' not found", err.ARN)
}

// ErrInvalidARN - invalid ARN error.
type ErrInvalidARN struct {
	ARN string
}

func (err ErrInvalidARN) Error() string {
	return fmt.Sprintf("invalid ARN '%v'", err.ARN)
}
