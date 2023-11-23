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

package grid

import (
	"errors"
	"fmt"
)

var (
	// ErrUnknownHandler is returned when an unknown handler is requested.
	ErrUnknownHandler = errors.New("unknown mux handler")

	// ErrHandlerAlreadyExists is returned when a handler is already registered.
	ErrHandlerAlreadyExists = errors.New("mux handler already exists")

	// ErrIncorrectSequence is returned when an out-of-sequence item is received.
	ErrIncorrectSequence = errors.New("out-of-sequence item received")
)

// ErrResponse is a remote error response.
type ErrResponse struct {
	msg string
}

func (e ErrResponse) Error() string {
	return fmt.Sprintf("remote: %s", e.msg)
}
