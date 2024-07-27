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

package dsync

//go:generate msgp -file $GOFILE

// LockArgs is minimal required values for any dsync compatible lock operation.
type LockArgs struct {
	// Unique ID of lock/unlock request.
	UID string

	// Resources contains single or multiple entries to be locked/unlocked.
	Resources []string

	// Owner represents unique ID for this instance, an owner who originally requested
	// the locked resource, useful primarily in figuring out stale locks.
	Owner string

	// Source contains the line number, function and file name of the code
	// on the client node that requested the lock.
	Source string `msgp:"omitempty"`

	// Quorum represents the expected quorum for this lock type.
	Quorum *int `msgp:"omitempty"`
}

// ResponseCode is the response code for a locking request.
type ResponseCode uint8

// Response codes for a locking request.
const (
	RespOK ResponseCode = iota
	RespLockConflict
	RespLockNotInitialized
	RespLockNotFound
	RespErr
)

// LockResp is a locking request response.
type LockResp struct {
	Code ResponseCode
	Err  string
}
