// Copyright (c) 2015-2025 MinIO, Inc.
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
package useragent

import (
	"sync/atomic"
)

type providerFunc func() string

var provider atomic.Pointer[providerFunc]

// Register sets a custom function to provide the User-Agent string.
func Register(f func() string) {
	p := providerFunc(f)
	provider.Store(&p)
}

// Get returns the current User-Agent string.
func Get() string {
	if fn := provider.Load(); fn != nil {
		return (*fn)()
	}
	return "minio/go-http"
}
