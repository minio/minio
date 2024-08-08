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

// getFileInfoVersions partitions this object's versions such that,
//   - fivs.Versions has all the non-free versions
//   - fivs.FreeVersions has all the free versions
//
// if inclFreeVersions is true all the versions are in fivs.Versions, free and non-free versions alike.
//
// Note: Only the scanner requires fivs.Versions to have exclusively non-free versions. This is used while enforcing NewerNoncurrentVersions lifecycle element.
func getFileInfoVersions(xlMetaBuf []byte, volume, path string, inclFreeVersions bool) (FileInfoVersions, error) {
	fivs, err := getAllFileInfoVersions(xlMetaBuf, volume, path, true)
	if err != nil {
		return fivs, err
	}

	// If inclFreeVersions is false, partition the versions in fivs.Versions
	// such that finally fivs.Versions has
	// all the non-free versions and fivs.FreeVersions has all the free
	// versions.
	n := 0
	for _, fi := range fivs.Versions {
		// filter our tier object delete marker
		if fi.TierFreeVersion() {
			if !inclFreeVersions {
				fivs.FreeVersions = append(fivs.FreeVersions, fi)
			}
		} else {
			if !inclFreeVersions {
				fivs.Versions[n] = fi
			}
			n++
		}
	}
	if !inclFreeVersions {
		fivs.Versions = fivs.Versions[:n]
	}
	// Update numversions
	for i := range fivs.Versions {
		fivs.Versions[i].NumVersions = n
	}
	return fivs, nil
}

func getAllFileInfoVersions(xlMetaBuf []byte, volume, path string, allParts bool) (FileInfoVersions, error) {
	var versions []FileInfo
	var err error

	if buf, _, e := isIndexedMetaV2(xlMetaBuf); e != nil {
		return FileInfoVersions{}, e
	} else if buf != nil {
		versions, err = buf.ListVersions(volume, path, allParts)
	} else {
		var xlMeta xlMetaV2
		if err := xlMeta.LoadOrConvert(xlMetaBuf); err != nil {
			return FileInfoVersions{}, err
		}
		versions, err = xlMeta.ListVersions(volume, path, allParts)
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

type fileInfoOpts struct {
	InclFreeVersions bool
	Data             bool
}

func getFileInfo(xlMetaBuf []byte, volume, path, versionID string, opts fileInfoOpts) (FileInfo, error) {
	var fi FileInfo
	var err error
	var inData xlMetaInlineData
	if buf, data, e := isIndexedMetaV2(xlMetaBuf); e != nil {
		return FileInfo{}, e
	} else if buf != nil {
		inData = data
		fi, err = buf.ToFileInfo(volume, path, versionID, true)
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
		fi, err = xlMeta.ToFileInfo(volume, path, versionID, opts.InclFreeVersions, true)
	}
	if !opts.Data || err != nil {
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
