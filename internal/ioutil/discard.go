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

package ioutil

import (
	"io"
)

// Discard is just like io.Discard without the io.ReaderFrom compatible
// implementation which is buggy on NUMA systems, we have to use a simpler
// io.Writer implementation alone avoids also unnecessary buffer copies,
// and as such incurred latencies.
var Discard io.Writer = discard{}

// discard is /dev/null for Golang.
type discard struct{}

func (discard) Write(p []byte) (int, error) {
	return len(p), nil
}

// DiscardReader discarded reader
func DiscardReader(r io.Reader) {
	Copy(Discard, r)
}
