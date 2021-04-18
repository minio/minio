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

package hash

import "fmt"

// SHA256Mismatch - when content sha256 does not match with what was sent from client.
type SHA256Mismatch struct {
	ExpectedSHA256   string
	CalculatedSHA256 string
}

func (e SHA256Mismatch) Error() string {
	return "Bad sha256: Expected " + e.ExpectedSHA256 + " does not match calculated " + e.CalculatedSHA256
}

// BadDigest - Content-MD5 you specified did not match what we received.
type BadDigest struct {
	ExpectedMD5   string
	CalculatedMD5 string
}

func (e BadDigest) Error() string {
	return "Bad digest: Expected " + e.ExpectedMD5 + " does not match calculated " + e.CalculatedMD5
}

// ErrSizeMismatch error size mismatch
type ErrSizeMismatch struct {
	Want int64
	Got  int64
}

func (e ErrSizeMismatch) Error() string {
	return fmt.Sprintf("Size mismatch: got %d, want %d", e.Got, e.Want)
}
