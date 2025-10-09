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

package rest

import (
	"errors"
	"net"
	"net/url"
	"testing"
)

func TestNetworkError_Unwrap(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target any
		want   bool
	}{
		{
			name:   "url.Error",
			err:    &url.Error{Op: "PUT", URL: "http://localhost/1234", Err: restError("remote server offline")},
			target: &url.Error{},
			want:   true,
		},
		{
			name: "net.Error",
			err:  &url.Error{Op: "PUT", URL: "http://localhost/1234", Err: restError("remote server offline")},
			want: true,
		},
		{
			name: "net.Error-unmatched",
			err:  errors.New("something"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Wrap error
			n := &NetworkError{
				Err: tt.err,
			}
			//nolint:gocritic
			if tt.target == nil {
				var netErrInterface net.Error
				if errors.As(n, &netErrInterface) != tt.want {
					t.Errorf("errors.As(n, &tt.target) != tt.want, n: %#v, target: %#v, want:%v, got: %v", n, tt.target, tt.want, !tt.want)
				}
			} else {
				if errors.As(n, &tt.target) != tt.want {
					t.Errorf("errors.As(n, &tt.target) != tt.want, n: %#v, target: %#v, want:%v, got: %v", n, tt.target, tt.want, !tt.want)
				}
			}
		})
	}
}
