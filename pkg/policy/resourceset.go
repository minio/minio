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

package policy

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/minio/minio-go/pkg/set"
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

// Intersection - returns resouces available in both ResourcsSet.
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
		return nil, fmt.Errorf("empty resource set")
	}

	resources := []Resource{}
	for resource := range resourceSet {
		resources = append(resources, resource)
	}

	return json.Marshal(resources)
}

// Match - matches object name with anyone of resource pattern in resource set.
func (resourceSet ResourceSet) Match(resource string) bool {
	for r := range resourceSet {
		if r.Match(resource) {
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
			return fmt.Errorf("duplicate resource '%v' found", s)
		}

		resourceSet.Add(resource)
	}

	return nil
}

// Validate - validates ResourceSet is for given bucket or not.
func (resourceSet ResourceSet) Validate(bucketName string) error {
	for resource := range resourceSet {
		if err := resource.Validate(bucketName); err != nil {
			return err
		}
	}

	return nil
}

// NewResourceSet - creates new resource set.
func NewResourceSet(resources ...Resource) ResourceSet {
	resourceSet := make(ResourceSet)
	for _, resource := range resources {
		resourceSet.Add(resource)
	}

	return resourceSet
}
