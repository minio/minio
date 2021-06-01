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

// RulesMap - map of rules for every event name.
type RulesMap map[Name]Rules

// add - adds event names, prefixes, suffixes and target ID to rules map.
func (rulesMap RulesMap) add(eventNames []Name, pattern string, targetID TargetID) {
	rules := make(Rules)
	rules.Add(pattern, targetID)

	for _, eventName := range eventNames {
		for _, name := range eventName.Expand() {
			rulesMap[name] = rulesMap[name].Union(rules)
		}
	}
}

// Clone - returns copy of this rules map.
func (rulesMap RulesMap) Clone() RulesMap {
	rulesMapCopy := make(RulesMap)

	for eventName, rules := range rulesMap {
		rulesMapCopy[eventName] = rules.Clone()
	}

	return rulesMapCopy
}

// Add - adds given rules map.
func (rulesMap RulesMap) Add(rulesMap2 RulesMap) {
	for eventName, rules := range rulesMap2 {
		rulesMap[eventName] = rules.Union(rulesMap[eventName])
	}
}

// Remove - removes given rules map.
func (rulesMap RulesMap) Remove(rulesMap2 RulesMap) {
	for eventName, rules := range rulesMap {
		if nr := rules.Difference(rulesMap2[eventName]); len(nr) != 0 {
			rulesMap[eventName] = nr
		} else {
			delete(rulesMap, eventName)
		}
	}
}

// MatchSimple - returns true if matching object name and event name in rules map.
func (rulesMap RulesMap) MatchSimple(eventName Name, objectName string) bool {
	return rulesMap[eventName].MatchSimple(objectName)
}

// Match - returns TargetIDSet matching object name and event name in rules map.
func (rulesMap RulesMap) Match(eventName Name, objectName string) TargetIDSet {
	return rulesMap[eventName].Match(objectName)
}

// NewRulesMap - creates new rules map with given values.
func NewRulesMap(eventNames []Name, pattern string, targetID TargetID) RulesMap {
	// If pattern is empty, add '*' wildcard to match all.
	if pattern == "" {
		pattern = "*"
	}

	rulesMap := make(RulesMap)
	rulesMap.add(eventNames, pattern, targetID)
	return rulesMap
}
