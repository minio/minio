/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package event

import "fmt"

// TargetIDSet - Set representation of TargetIDs.
type TargetIDSet map[TargetID]struct{}

// ToSlice - returns TargetID slice from TargetIDSet.
func (set TargetIDSet) ToSlice() []TargetID {
	keys := make([]TargetID, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys
}

// String - returns string representation.
func (set TargetIDSet) String() string {
	return fmt.Sprintf("%v", set.ToSlice())
}

// Clone - returns copy of this set.
func (set TargetIDSet) Clone() TargetIDSet {
	setCopy := NewTargetIDSet()
	for k, v := range set {
		setCopy[k] = v
	}
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

// Difference - returns diffrence with given set as new set.
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
