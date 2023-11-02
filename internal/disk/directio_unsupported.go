//go:build windows || openbsd || plan9

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

package disk

import (
	"os"
)

// ODirectPlatform indicates if the platform supports O_DIRECT
const ODirectPlatform = false

// OpenBSD, Windows, and illumos do not support O_DIRECT.
// On Windows there is no documentation on disabling O_DIRECT.
// For these systems we do not attempt to build the 'directio' dependency since
// the O_DIRECT symbol may not be exposed resulting in a failed build.
//
//
// On illumos an explicit O_DIRECT flag is not necessary for two primary
// reasons. Note that ZFS is effectively the default filesystem on illumos
// systems.
//
// One benefit of using DirectIO on Linux is that the page cache will not be
// polluted with single-access data. The ZFS read cache (ARC) is scan-resistant
// so there is no risk of polluting the entire cache with data accessed once.
// Another goal of DirectIO is to minimize the mutation of data by the kernel
// before issuing IO to underlying devices. ZFS users often enable features like
// compression and checksumming which currently necessitates mutating data in
// the kernel.
//
// DirectIO semantics for a filesystem like ZFS would be quite different than
// the semantics on filesystems like XFS, and these semantics are not
// implemented at this time.
// For more information on why typical DirectIO semantics do not apply to ZFS
// see this ZFS-on-Linux commit message:
// https://github.com/openzfs/zfs/commit/a584ef26053065f486d46a7335bea222cb03eeea

// OpenFileDirectIO wrapper around os.OpenFile nothing special
func OpenFileDirectIO(filePath string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filePath, flag, perm)
}

// DisableDirectIO is a no-op
func DisableDirectIO(f *os.File) error {
	return nil
}

// AlignedBlock simply returns an unaligned buffer
// for systems that do not support DirectIO.
func AlignedBlock(blockSize int) []byte {
	return make([]byte, blockSize)
}
