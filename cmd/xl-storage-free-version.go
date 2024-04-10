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
	"bytes"
	"fmt"

	"github.com/google/uuid"
	"github.com/minio/minio/internal/bucket/lifecycle"
)

const freeVersion = "free-version"

// InitFreeVersion creates a free-version to track the tiered-content of j. If j has
// no tiered content, it returns false.
func (j xlMetaV2Object) InitFreeVersion(fi FileInfo) (xlMetaV2Version, bool) {
	if fi.SkipTierFreeVersion() {
		return xlMetaV2Version{}, false
	}
	if status, ok := j.MetaSys[ReservedMetadataPrefixLower+TransitionStatus]; ok && bytes.Equal(status, []byte(lifecycle.TransitionComplete)) {
		vID, err := uuid.Parse(fi.TierFreeVersionID())
		if err != nil {
			panic(fmt.Errorf("Invalid Tier Object delete marker versionId %s %v", fi.TierFreeVersionID(), err))
		}
		freeEntry := xlMetaV2Version{Type: DeleteType, WrittenByVersion: globalVersionUnix}
		freeEntry.DeleteMarker = &xlMetaV2DeleteMarker{
			VersionID: vID,
			ModTime:   j.ModTime, // fi.ModTime may be empty
			MetaSys:   make(map[string][]byte),
		}

		freeEntry.DeleteMarker.MetaSys[ReservedMetadataPrefixLower+freeVersion] = []byte{}
		tierKey := ReservedMetadataPrefixLower + TransitionTier
		tierObjKey := ReservedMetadataPrefixLower + TransitionedObjectName
		tierObjVIDKey := ReservedMetadataPrefixLower + TransitionedVersionID

		for k, v := range j.MetaSys {
			switch k {
			case tierKey, tierObjKey, tierObjVIDKey:
				freeEntry.DeleteMarker.MetaSys[k] = v
			}
		}
		return freeEntry, true
	}
	return xlMetaV2Version{}, false
}

// FreeVersion returns true if j represents a free-version, false otherwise.
func (j xlMetaV2DeleteMarker) FreeVersion() bool {
	_, ok := j.MetaSys[ReservedMetadataPrefixLower+freeVersion]
	return ok
}

// FreeVersion returns true if j represents a free-version, false otherwise.
func (j xlMetaV2Version) FreeVersion() bool {
	if j.Type == DeleteType {
		return j.DeleteMarker.FreeVersion()
	}
	return false
}

// AddFreeVersion adds a free-version if needed for fi.VersionID version.
// Free-version will be added if fi.VersionID has transitioned.
func (x *xlMetaV2) AddFreeVersion(fi FileInfo) error {
	var uv uuid.UUID
	var err error
	switch fi.VersionID {
	case "", nullVersionID:
	default:
		uv, err = uuid.Parse(fi.VersionID)
		if err != nil {
			return err
		}
	}

	for i, version := range x.versions {
		if version.header.VersionID != uv || version.header.Type != ObjectType {
			continue
		}
		// if uv has tiered content we add a
		// free-version to track it for asynchronous
		// deletion via scanner.
		ver, err := x.getIdx(i)
		if err != nil {
			return err
		}

		if freeVersion, toFree := ver.ObjectV2.InitFreeVersion(fi); toFree {
			return x.addVersion(freeVersion)
		}
		return nil
	}
	return nil
}
