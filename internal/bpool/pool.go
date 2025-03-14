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

package bpool

import "sync"

// Pool is a single type sync.Pool with a few extra properties:
// If New is not set Get may return the zero value of T.
type Pool[T any] struct {
	New func() T
	p   sync.Pool
}

// Get will retuen a new T
func (p *Pool[T]) Get() T {
	v, ok := p.p.Get().(T)
	if ok {
		return v
	}
	if p.New == nil {
		var t T
		return t
	}
	return p.New()
}

// Put a used T.
func (p *Pool[T]) Put(t T) {
	p.p.Put(t)
}
