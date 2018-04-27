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

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/wildcard"
)

// Principal - policy principal.
type Principal struct {
	AWS set.StringSet
}

// IsValid - checks whether Principal is valid or not.
func (p Principal) IsValid() bool {
	return len(p.AWS) != 0
}

// Intersection - returns principals available in both Principal.
func (p Principal) Intersection(principal Principal) set.StringSet {
	return p.AWS.Intersection(principal.AWS)
}

// MarshalJSON - encodes Principal to JSON data.
func (p Principal) MarshalJSON() ([]byte, error) {
	if !p.IsValid() {
		return nil, fmt.Errorf("invalid principal %v", p)
	}

	// subtype to avoid recursive call to MarshalJSON()
	type subPrincipal Principal
	sp := subPrincipal(p)
	return json.Marshal(sp)
}

// Match - matches given principal is wildcard matching with Principal.
func (p Principal) Match(principal string) bool {
	for _, pattern := range p.AWS.ToSlice() {
		if wildcard.MatchSimple(pattern, principal) {
			return true
		}
	}

	return false
}

// UnmarshalJSON - decodes JSON data to Principal.
func (p *Principal) UnmarshalJSON(data []byte) error {
	// subtype to avoid recursive call to UnmarshalJSON()
	type subPrincipal Principal
	var sp subPrincipal

	if err := json.Unmarshal(data, &sp); err != nil {
		var s string
		if err = json.Unmarshal(data, &s); err != nil {
			return err
		}

		if s != "*" {
			return fmt.Errorf("invalid principal '%v'", s)
		}

		sp.AWS = set.CreateStringSet("*")
	}

	*p = Principal(sp)

	return nil
}

// NewPrincipal - creates new Principal.
func NewPrincipal(principals ...string) Principal {
	return Principal{AWS: set.CreateStringSet(principals...)}
}
