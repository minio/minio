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

package iampolicy

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/minio/minio-go/v7/pkg/set"
)

// ResourceSet - set of resources in policy statement.
type ResourceSet map[Resource]struct{}

// bucketResourceExists - checks if at least one bucket resource exists in the set.
func (resourceSet ResourceSet) bucketResourceExists() bool {
	for resource := range resourceSet {
		if resource.isBucketPattern() {
			return true
		}
	}

	return false
}

// objectResourceExists - checks if at least one object resource exists in the set.
func (resourceSet ResourceSet) objectResourceExists() bool {
	for resource := range resourceSet {
		if resource.isObjectPattern() {
			return true
		}
	}

	return false
}

// Add - adds resource to resource set.
func (resourceSet ResourceSet) Add(resource Resource) {
	resourceSet[resource] = struct{}{}
}

// Equals - checks whether given resource set is equal to current resource set or not.
func (resourceSet ResourceSet) Equals(sresourceSet ResourceSet) bool {
	// If length of set is not equal to length of given set, the
	// set is not equal to given set.
	if len(resourceSet) != len(sresourceSet) {
		return false
	}

	// As both sets are equal in length, check each elements are equal.
	for k := range resourceSet {
		if _, ok := sresourceSet[k]; !ok {
			return false
		}
	}

	return true
}

// Intersection - returns resources available in both ResourceSet.
func (resourceSet ResourceSet) Intersection(sset ResourceSet) ResourceSet {
	nset := NewResourceSet()
	for k := range resourceSet {
		if _, ok := sset[k]; ok {
			nset.Add(k)
		}
	}

	return nset
}

// MarshalJSON - encodes ResourceSet to JSON data.
func (resourceSet ResourceSet) MarshalJSON() ([]byte, error) {
	if len(resourceSet) == 0 {
		return nil, Errorf("empty resource set")
	}

	resources := []Resource{}
	for resource := range resourceSet {
		resources = append(resources, resource)
	}

	return json.Marshal(resources)
}

// MatchResource matches object name with resource patterns only.
func (resourceSet ResourceSet) MatchResource(resource string) bool {
	for r := range resourceSet {
		if r.MatchResource(resource) {
			return true
		}
	}
	return false
}

// Match - matches object name with anyone of resource pattern in resource set.
func (resourceSet ResourceSet) Match(resource string, conditionValues map[string][]string) bool {
	for r := range resourceSet {
		if r.Match(resource, conditionValues) {
			return true
		}
	}

	return false
}

func (resourceSet ResourceSet) String() string {
	resources := []string{}
	for resource := range resourceSet {
		resources = append(resources, resource.String())
	}
	sort.Strings(resources)

	return fmt.Sprintf("%v", resources)
}

// UnmarshalJSON - decodes JSON data to ResourceSet.
func (resourceSet *ResourceSet) UnmarshalJSON(data []byte) error {
	var sset set.StringSet
	if err := json.Unmarshal(data, &sset); err != nil {
		return err
	}

	*resourceSet = make(ResourceSet)
	for _, s := range sset.ToSlice() {
		resource, err := parseResource(s)
		if err != nil {
			return err
		}

		if _, found := (*resourceSet)[resource]; found {
			return Errorf("duplicate resource '%v' found", s)
		}

		resourceSet.Add(resource)
	}

	return nil
}

// Validate - validates ResourceSet.
func (resourceSet ResourceSet) Validate() error {
	for resource := range resourceSet {
		if err := resource.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// ToSlice - returns slice of resources from the resource set.
func (resourceSet ResourceSet) ToSlice() []Resource {
	resources := []Resource{}
	for resource := range resourceSet {
		resources = append(resources, resource)
	}

	return resources
}

// Clone clones ResourceSet structure
func (resourceSet ResourceSet) Clone() ResourceSet {
	return NewResourceSet(resourceSet.ToSlice()...)
}

// NewResourceSet - creates new resource set.
func NewResourceSet(resources ...Resource) ResourceSet {
	resourceSet := make(ResourceSet)
	for _, resource := range resources {
		resourceSet.Add(resource)
	}

	return resourceSet
}
