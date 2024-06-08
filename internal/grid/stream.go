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
// If the call is canceled through the context,
// the appropriate error will be returned.
type Stream struct {
	// responses from the remote server.
	// Channel will be closed after error or when remote closes.
	// All responses *must* be read by the caller until either an error is returned or the channel is closed.
	// Canceling the context will cause the context cancellation error to be returned.
	responses <-chan Response
	cancel    context.CancelCauseFunc

	// Requests sent to the server.
	// If the handler is defined with 0 incoming capacity this will be nil.
	// Channel *must* be closed to signal the end of the stream.
	// If the request context is canceled, the stream will no longer process requests.
	// Requests sent cannot be used any further by the called.
	Requests chan<- []byte

	muxID uint64
	ctx   context.Context
}

// Send a payload to the remote server.
func (s *Stream) Send(b []byte) error {
	if s.Requests == nil {
		return errors.New("stream does not accept requests")
	}
	select {
	case s.Requests <- b:
		return nil
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	}
}

// Results returns the results from the remote server one by one.
// If any error is returned by the callback, the stream will be canceled.
// If the context is canceled, the stream will be canceled.
func (s *Stream) Results(next func(b []byte) error) (err error) {
	done := false
	defer func() {
		if s.cancel != nil {
			s.cancel(err)
		}

		if !done {
			// Drain channel.
			for range s.responses {
			}
		}
	}()
	doneCh := s.ctx.Done()
	for {
		select {
		case <-doneCh:
			if err := context.Cause(s.ctx); !errors.Is(err, errStreamEOF) {
				return err
			}
			// Fall through to be sure we have returned all responses.
			doneCh = nil
		case resp, ok := <-s.responses:
			if !ok {
				done = true
				return nil
			}
			if resp.Err != nil {
				s.cancel(resp.Err)
				return resp.Err
			}
			err = next(resp.Msg)
			if err != nil {
				s.cancel(err)
				return err
			}
		}
	}
}

// Done will return a channel that will be closed when the stream is done.
// This mirrors context.Done().
func (s *Stream) Done() <-chan struct{} {
	return s.ctx.Done()
}

// Err will return the error that caused the stream to end.
// This mirrors context.Err().
func (s *Stream) Err() error {
	return s.ctx.Err()
}
