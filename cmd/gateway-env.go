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

type gatewaySSE []string

const (
	// GatewaySSES3 is set when SSE-S3 encryption needed on both gateway and backend
	gatewaySSES3 = "S3"
	// GatewaySSEC is set when SSE-C encryption needed on both gateway and backend
	gatewaySSEC = "C"
)

func (sse gatewaySSE) SSES3() bool {
	for _, v := range sse {
		if v == gatewaySSES3 {
			return true
		}
	}
	return false
}

func (sse gatewaySSE) SSEC() bool {
	for _, v := range sse {
		if v == gatewaySSEC {
			return true
		}
	}
	return false
}

func (sse gatewaySSE) IsSet() bool {
	return sse.SSES3() || sse.SSEC()
}
