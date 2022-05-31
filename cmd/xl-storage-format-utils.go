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

	"github.com/zeebo/xxh3"
)

func getFileInfoVersions(xlMetaBuf []byte, volume, path string) (FileInfoVersions, error) {
	fivs, err := getAllFileInfoVersions(xlMetaBuf, volume, path)
	if err != nil {
		return fivs, err
	}
	n := 0
	for _, fi := range fivs.Versions {
		// Filter our tier object delete marker
		if !fi.TierFreeVersion() {
			fivs.Versions[n] = fi
			n++
		} else {
			fivs.FreeVersions = append(fivs.FreeVersions, fi)
		}
	}
	fivs.Versions = fivs.Versions[:n]
	// Update numversions
	for i := range fivs.Versions {
		fivs.Versions[i].NumVersions = n
	}
	return fivs, nil
}

func getAllFileInfoVersions(xlMetaBuf []byte, volume, path string) (FileInfoVersions, error) {
	var versions []FileInfo
	var err error

	if buf, _, e := isIndexedMetaV2(xlMetaBuf); e != nil {
		return FileInfoVersions{}, e
	} else if buf != nil {
		versions, err = buf.ListVersions(volume, path)
	} else {
		var xlMeta xlMetaV2
		if err := xlMeta.LoadOrConvert(xlMetaBuf); err != nil {
			return FileInfoVersions{}, err
		}
		versions, err = xlMeta.ListVersions(volume, path)
	}
	if err == nil && len(versions) == 0 {
		// This special case is needed to handle len(xlMeta.versions) == 0
		versions = []FileInfo{
			{
				Volume:   volume,
				Name:     path,
				Deleted:  true,
				IsLatest: true,
				ModTime:  timeSentinel1970,
			},
		}
	}
	if err != nil {
		return FileInfoVersions{}, err
	}

	return FileInfoVersions{
		Volume:        volume,
		Name:          path,
		Versions:      versions,
		LatestModTime: versions[0].ModTime,
	}, nil
}

func getFileInfo(xlMetaBuf []byte, volume, path, versionID string, data bool) (FileInfo, error) {
	var fi FileInfo
	var err error
	var inData xlMetaInlineData
	if buf, data, e := isIndexedMetaV2(xlMetaBuf); e != nil {
		return FileInfo{}, e
	} else if buf != nil {
		inData = data
		fi, err = buf.ToFileInfo(volume, path, versionID)
		if len(buf) != 0 && errors.Is(err, errFileNotFound) {
			// This special case is needed to handle len(xlMeta.versions) == 0
			return FileInfo{
				Volume:    volume,
				Name:      path,
				VersionID: versionID,
				Deleted:   true,
				IsLatest:  true,
				ModTime:   timeSentinel1970,
			}, nil
		}
	} else {
		var xlMeta xlMetaV2
		if err := xlMeta.LoadOrConvert(xlMetaBuf); err != nil {
			return FileInfo{}, err
		}
		if len(xlMeta.versions) == 0 {
			// This special case is needed to handle len(xlMeta.versions) == 0
			return FileInfo{
				Volume:    volume,
				Name:      path,
				VersionID: versionID,
				Deleted:   true,
				IsLatest:  true,
				ModTime:   timeSentinel1970,
			}, nil
		}
		inData = xlMeta.data
		fi, err = xlMeta.ToFileInfo(volume, path, versionID)
	}
	if !data || err != nil {
		return fi, err
	}
	versionID = fi.VersionID
	if versionID == "" {
		versionID = nullVersionID
	}
	fi.Data = inData.find(versionID)
	if len(fi.Data) == 0 {
		// PR #11758 used DataDir, preserve it
		// for users who might have used master
		// branch
		fi.Data = inData.find(fi.DataDir)
	}
	return fi, nil
}

// getXLDiskLoc will return the pool/set/disk id if it can be located in the object layer.
// Will return -1 for unknown values.
func getXLDiskLoc(diskID string) (poolIdx, setIdx, diskIdx int) {
	if api := newObjectLayerFn(); api != nil {
		if globalIsErasureSD {
			return 0, 0, 0
		}
		if ep, ok := api.(*erasureServerPools); ok {
			if pool, set, disk, err := ep.getPoolAndSet(diskID); err == nil {
				return pool, set, disk
			}
		}
	}
	return -1, -1, -1
}

// hashDeterministicString will return a deterministic hash for the map values.
// Trivial collisions are avoided, but this is by no means a strong hash.
func hashDeterministicString(m map[string]string) uint64 {
	// Seed (random)
	crc := uint64(0xc2b40bbac11a7295)
	// Xor each value to make order independent
	for k, v := range m {
		// Separate key and value with an individual xor with a random number.
		// Add values of each, so they cannot be trivially collided.
		crc ^= (xxh3.HashString(k) ^ 0x4ee3bbaf7ab2506b) + (xxh3.HashString(v) ^ 0x8da4c8da66194257)
	}
	return crc
}

// hashDeterministicBytes will return a deterministic (weak) hash for the map values.
// Trivial collisions are avoided, but this is by no means a strong hash.
func hashDeterministicBytes(m map[string][]byte) uint64 {
	crc := uint64(0x1bbc7e1dde654743)
	for k, v := range m {
		crc ^= (xxh3.HashString(k) ^ 0x4ee3bbaf7ab2506b) + (xxh3.Hash(v) ^ 0x8da4c8da66194257)
	}
	return crc
}
