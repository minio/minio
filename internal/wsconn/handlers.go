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

package wsconn

import (
	"fmt"
)

const (
	// handlerInvalid is reserved to check for uninitialized values.
	handlerInvalid HandlerID = iota
	HandlerPing

	// Add more above.
	// If all handlers are used, the type of Handler can be changed.
	handlerLast
)

const (
	handlerVerShift = 8
	handlerIDMask   = (1 << handlerVerShift) - 1
)

func init() {
	// Static check if we exceed 255 handler ids.
	if handlerLast > handlerIDMask {
		panic(fmt.Sprintf("out of handler IDs. %d > %d", handlerLast, handlerIDMask))
	}
}

func (h HandlerID) valid() bool {
	id := HandlerID(h.ID())
	return id != handlerInvalid && id < handlerLast
}

// WithVersion sets a version on the handler that must be satisfied.
// If unset, initial version 0 is used.
func (h HandlerID) WithVersion(n uint8) HandlerID {
	return (h & handlerIDMask) | (HandlerID(n) << handlerVerShift)
}

// Version returns the version.
func (h HandlerID) Version() uint8 {
	return uint8(h >> handlerVerShift)
}

// ID returns the id without version.
func (h HandlerID) ID() uint8 {
	return uint8(h & handlerIDMask)
}

type StatelessHandler func(payload []byte) ([]byte, error)

type StatefulHandler func(request <-chan []byte, resp chan<- Response)
