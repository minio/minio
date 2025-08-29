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

package event

import "maps"

// TargetIDSet - Set representation of TargetIDs.
type TargetIDSet map[TargetID]struct{}

// Clone - returns copy of this set.
func (set TargetIDSet) Clone() TargetIDSet {
	setCopy := NewTargetIDSet()
	maps.Copy(setCopy, set)
	return setCopy
}

// add - adds TargetID to the set.
func (set TargetIDSet) add(targetID TargetID) {
	set[targetID] = struct{}{}
}

// Union - returns union with given set as new set.
func (set TargetIDSet) Union(sset TargetIDSet) TargetIDSet {
	nset := set.Clone()

	for k := range sset {
		nset.add(k)
	}

	return nset
}

// Difference - returns difference with given set as new set.
func (set TargetIDSet) Difference(sset TargetIDSet) TargetIDSet {
	nset := NewTargetIDSet()
	for k := range set {
		if _, ok := sset[k]; !ok {
			nset.add(k)
		}
	}

	return nset
}

// NewTargetIDSet - creates new TargetID set with given TargetIDs.
func NewTargetIDSet(targetIDs ...TargetID) TargetIDSet {
	set := make(TargetIDSet)
	for _, targetID := range targetIDs {
		set.add(targetID)
	}
	return set
}
