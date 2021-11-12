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
	jsoniter "github.com/json-iterator/go"
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
	if isXL2V1Format(xlMetaBuf) {
		var versions []FileInfo
		var err error
		if buf, _ := isIndexedMetaV2(xlMetaBuf); buf != nil {
			versions, err = buf.ListVersions(volume, path)
		} else {
			var xlMeta xlMetaV2
			if err := xlMeta.Load(xlMetaBuf); err != nil {
				return FileInfoVersions{}, err
			}
			versions, err = xlMeta.ListVersions(volume, path)
		}
		if err != nil || len(versions) == 0 {
			return FileInfoVersions{}, err
		}

		return FileInfoVersions{
			Volume:        volume,
			Name:          path,
			Versions:      versions,
			LatestModTime: versions[0].ModTime,
		}, nil
	}

	xlMeta := &xlMetaV1Object{}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(xlMetaBuf, xlMeta); err != nil {
		return FileInfoVersions{}, errFileCorrupt
	}

	fi, err := xlMeta.ToFileInfo(volume, path)
	if err != nil {
		return FileInfoVersions{}, err
	}

	fi.IsLatest = true // No versions so current version is latest.
	fi.XLV1 = true     // indicates older version
	return FileInfoVersions{
		Volume:        volume,
		Name:          path,
		Versions:      []FileInfo{fi},
		LatestModTime: fi.ModTime,
	}, nil
}

func getFileInfo(xlMetaBuf []byte, volume, path, versionID string, data bool) (FileInfo, error) {
	if isXL2V1Format(xlMetaBuf) {
		var fi FileInfo
		var err error
		var inData xlMetaInlineData
		if buf, data := isIndexedMetaV2(xlMetaBuf); buf != nil {
			inData = data
			fi, err = buf.ToFileInfo(volume, path, versionID)
		} else {
			var xlMeta xlMetaV2
			if err := xlMeta.Load(xlMetaBuf); err != nil {
				return FileInfo{}, err
			}
			inData = xlMeta.data
			fi, err = xlMeta.ToFileInfo(volume, path, versionID)
		}
		if !data || err != nil {
			return fi, err
		}
		versionID := fi.VersionID
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

	xlMeta := &xlMetaV1Object{}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(xlMetaBuf, xlMeta); err != nil {
		return FileInfo{}, errFileCorrupt
	}

	fi, err := xlMeta.ToFileInfo(volume, path)
	if err == errFileNotFound && versionID != "" {
		return fi, errFileVersionNotFound
	}
	fi.IsLatest = true // No versions so current version is latest.
	fi.XLV1 = true     // indicates older version
	return fi, err
}

// getXLDiskLoc will return the pool/set/disk id if it can be located in the object layer.
// Will return -1 for unknown values.
func getXLDiskLoc(diskID string) (poolIdx, setIdx, diskIdx int) {
	if api := newObjectLayerFn(); api != nil {
		if ep, ok := api.(*erasureServerPools); ok {
			if pool, set, disk, err := ep.getPoolAndSet(diskID); err == nil {
				return pool, set, disk
			}
		}
	}
	return -1, -1, -1
}

// hashDeterministicString will return a deterministic (weak) hash for the map values.
func hashDeterministicString(m map[string]string) uint64 {
	var crc = uint64(0xc2b40bbac11a7295)
	for k, v := range m {
		crc = crc ^ (xxh3.HashString(k) + xxh3.HashString(v))
	}
	return crc
}

// hashDeterministicBytes will return a deterministic (weak) hash for the map values.
func hashDeterministicBytes(m map[string][]byte) uint64 {
	var crc = uint64(0x1bbc7e1dde654743)
	for k, v := range m {
		crc = crc ^ (xxh3.HashString(k) + xxh3.Hash(v))
	}
	return crc
}
