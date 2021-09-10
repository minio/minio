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

import (
	"strings"

	"github.com/minio/pkg/wildcard"
)

// NewPattern - create new pattern for prefix/suffix.
func NewPattern(prefix, suffix string) (pattern string) {
	if prefix != "" {
		if !strings.HasSuffix(prefix, "*") {
			prefix += "*"
		}

		pattern = prefix
	}

	if suffix != "" {
		if !strings.HasPrefix(suffix, "*") {
			suffix = "*" + suffix
		}

		pattern += suffix
	}

	pattern = strings.Replace(pattern, "**", "*", -1)

	return pattern
}

// Rules - event rules
type Rules map[string]TargetIDSet

// Add - adds pattern and target ID.
func (rules Rules) Add(pattern string, targetID TargetID) {
	rules[pattern] = NewTargetIDSet(targetID).Union(rules[pattern])
}

// MatchSimple - returns true one of the matching object name in rules.
func (rules Rules) MatchSimple(objectName string) bool {
	for pattern := range rules {
		if wildcard.MatchSimple(pattern, objectName) {
			return true
		}
	}
	return false
}

// Match - returns TargetIDSet matching object name in rules.
func (rules Rules) Match(objectName string) TargetIDSet {
	targetIDs := NewTargetIDSet()

	for pattern, targetIDSet := range rules {
		if wildcard.MatchSimple(pattern, objectName) {
			targetIDs = targetIDs.Union(targetIDSet)
		}
	}

	return targetIDs
}

// Clone - returns copy of this rules.
func (rules Rules) Clone() Rules {
	rulesCopy := make(Rules)

	for pattern, targetIDSet := range rules {
		rulesCopy[pattern] = targetIDSet.Clone()
	}

	return rulesCopy
}

// Union - returns union with given rules as new rules.
func (rules Rules) Union(rules2 Rules) Rules {
	nrules := rules.Clone()

	for pattern, targetIDSet := range rules2 {
		nrules[pattern] = nrules[pattern].Union(targetIDSet)
	}

	return nrules
}

// Difference - returns diffrence with given rules as new rules.
func (rules Rules) Difference(rules2 Rules) Rules {
	nrules := make(Rules)

	for pattern, targetIDSet := range rules {
		if nv := targetIDSet.Difference(rules2[pattern]); len(nv) > 0 {
			nrules[pattern] = nv
		}
	}

	return nrules
}
