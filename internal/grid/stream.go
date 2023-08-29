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
	"context"
	"errors"
)

// A Stream is a two-way stream.
// All responses *must* be read by the caller.
// If the call is canceled though the context,
// the appropriate error will be returned.
type Stream struct {
	// Responses from the remote server.
	// Channel will be closed after error or when remote closes.
	// All responses *must* be read by the caller until either an error is returned or the channel is closed.
	// Canceling the context will cause the context cancellation error to be returned.
	Responses <-chan Response

	// Requests sent to the server.
	// If the handler is defined with 0 incoming capacity this will be nil.
	// Channel *must* be closed to signal the end of the stream.
	// If the request context is canceled, the stream will no longer process requests.
	Requests chan<- []byte

	ctx context.Context
}

func (s *Stream) Send(b []byte) error {
	if s.Requests == nil {
		return errors.New("stream does not accept requests")
	}
	select {
	case s.Requests <- b:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
