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

import (
	"errors"
)

const (
	lockRESTVersion       = "v7" // Add msgp for lockArgs
	lockRESTVersionPrefix = SlashSeparator + lockRESTVersion
	lockRESTPrefix        = minioReservedBucketPath + "/lock"
)

const (
	lockRESTMethodHealth      = "/health"
	lockRESTMethodRefresh     = "/refresh"
	lockRESTMethodLock        = "/lock"
	lockRESTMethodRLock       = "/rlock"
	lockRESTMethodUnlock      = "/unlock"
	lockRESTMethodRUnlock     = "/runlock"
	lockRESTMethodForceUnlock = "/force-unlock"
)

var (
	errLockConflict       = errors.New("lock conflict")
	errLockNotInitialized = errors.New("lock not initialized")
	errLockNotFound       = errors.New("lock not found")
)
