// +build !linux,!netbsd,!freebsd,!darwin

/*
 * Minio Cloud Storage, (C) 2019-2020 Minio, Inc.
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

package disk

import (
	"os"
)

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

func OpenFileDirectIO(filePath string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filePath, flag, perm)
}

func DisableDirectIO(f *os.File) error {
	return nil
}

// AlignedBlock simply returns an unaligned buffer for systems that do not
// support DirectIO.
func AlignedBlock(BlockSize int) []byte {
	return make([]byte, BlockSize)
}
