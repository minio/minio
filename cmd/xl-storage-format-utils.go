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
	"sort"

	jsoniter "github.com/json-iterator/go"
)

// versionsSorter sorts FileInfo slices by version.
type versionsSorter []FileInfo

func (v versionsSorter) sort() {
	sort.Slice(v, func(i, j int) bool {
		if v[i].IsLatest {
			return true
		}
		if v[j].IsLatest {
			return false
		}
		return v[i].ModTime.After(v[j].ModTime)
	})
}

func getFileInfoVersions(xlMetaBuf []byte, volume, path string) (FileInfoVersions, error) {
	if isXL2V1Format(xlMetaBuf) {
		var xlMeta xlMetaV2
		if err := xlMeta.Load(xlMetaBuf); err != nil {
			return FileInfoVersions{}, err
		}
		versions, latestModTime, err := xlMeta.ListVersions(volume, path)
		if err != nil {
			return FileInfoVersions{}, err
		}
		return FileInfoVersions{
			Volume:        volume,
			Name:          path,
			Versions:      versions,
			LatestModTime: latestModTime,
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
		var xlMeta xlMetaV2
		if err := xlMeta.Load(xlMetaBuf); err != nil {
			return FileInfo{}, err
		}
		fi, err := xlMeta.ToFileInfo(volume, path, versionID)
		if !data || err != nil {
			return fi, err
		}
		versionID := fi.VersionID
		if versionID == "" {
			versionID = nullVersionID
		}
		fi.Data = xlMeta.data.find(versionID)
		if len(fi.Data) == 0 {
			// PR #11758 used DataDir, preserve it
			// for users who might have used master
			// branch
			fi.Data = xlMeta.data.find(fi.DataDir)
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
