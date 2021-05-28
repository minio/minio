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

package policy

import (
	"encoding/json"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/pkg/wildcard"
)

// Principal - policy principal.
type Principal struct {
	AWS set.StringSet
}

// IsValid - checks whether Principal is valid or not.
func (p Principal) IsValid() bool {
	return len(p.AWS) != 0
}

// Equals - returns true if principals are equal.
func (p Principal) Equals(pp Principal) bool {
	return p.AWS.Equals(pp.AWS)
}

// Intersection - returns principals available in both Principal.
func (p Principal) Intersection(principal Principal) set.StringSet {
	return p.AWS.Intersection(principal.AWS)
}

// MarshalJSON - encodes Principal to JSON data.
func (p Principal) MarshalJSON() ([]byte, error) {
	if !p.IsValid() {
		return nil, Errorf("invalid principal %v", p)
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
			return Errorf("invalid principal '%v'", s)
		}

		sp.AWS = set.CreateStringSet("*")
	}

	*p = Principal(sp)

	return nil
}

// Clone clones Principal structure
func (p Principal) Clone() Principal {
	return NewPrincipal(p.AWS.ToSlice()...)

}

// NewPrincipal - creates new Principal.
func NewPrincipal(principals ...string) Principal {
	return Principal{AWS: set.CreateStringSet(principals...)}
}
