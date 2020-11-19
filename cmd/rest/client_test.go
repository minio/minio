/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
		target interface{}
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
