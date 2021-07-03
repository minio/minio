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
	"reflect"
	"testing"
)

func TestNewPattern(t *testing.T) {
	testCases := []struct {
		prefix         string
		suffix         string
		expectedResult string
	}{
		{"", "", ""},
		{"*", "", "*"},
		{"", "*", "*"},
		{"images/", "", "images/*"},
		{"images/*", "", "images/*"},
		{"", "jpg", "*jpg"},
		{"", "*jpg", "*jpg"},
		{"images/", "jpg", "images/*jpg"},
		{"images/*", "jpg", "images/*jpg"},
		{"images/", "*jpg", "images/*jpg"},
		{"images/*", "*jpg", "images/*jpg"},
		{"201*/images/", "jpg", "201*/images/*jpg"},
	}

	for i, testCase := range testCases {
		result := NewPattern(testCase.prefix, testCase.suffix)

		if result != testCase.expectedResult {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestRulesAdd(t *testing.T) {
	rulesCase1 := make(Rules)

	rulesCase2 := make(Rules)
	rulesCase2.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})

	rulesCase3 := make(Rules)
	rulesCase3.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})

	rulesCase4 := make(Rules)
	rulesCase4.Add(NewPattern("", "*.jpg"), TargetID{"1", "webhook"})

	rulesCase5 := make(Rules)

	rulesCase6 := make(Rules)
	rulesCase6.Add(NewPattern("", "*.jpg"), TargetID{"1", "webhook"})

	rulesCase7 := make(Rules)
	rulesCase7.Add(NewPattern("", "*.jpg"), TargetID{"1", "webhook"})

	rulesCase8 := make(Rules)
	rulesCase8.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})

	testCases := []struct {
		rules          Rules
		pattern        string
		targetID       TargetID
		expectedResult int
	}{
		{rulesCase1, NewPattern("*", ""), TargetID{"1", "webhook"}, 1},
		{rulesCase2, NewPattern("*", ""), TargetID{"2", "amqp"}, 2},
		{rulesCase3, NewPattern("2010*", ""), TargetID{"1", "webhook"}, 1},
		{rulesCase4, NewPattern("*", ""), TargetID{"1", "webhook"}, 2},
		{rulesCase5, NewPattern("", "*.jpg"), TargetID{"1", "webhook"}, 1},
		{rulesCase6, NewPattern("", "*"), TargetID{"2", "amqp"}, 2},
		{rulesCase7, NewPattern("", "*.jpg"), TargetID{"1", "webhook"}, 1},
		{rulesCase8, NewPattern("", "*.jpg"), TargetID{"1", "webhook"}, 2},
	}

	for i, testCase := range testCases {
		testCase.rules.Add(testCase.pattern, testCase.targetID)
		result := len(testCase.rules)

		if result != testCase.expectedResult {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestRulesMatch(t *testing.T) {
	rulesCase1 := make(Rules)

	rulesCase2 := make(Rules)
	rulesCase2.Add(NewPattern("*", "*"), TargetID{"1", "webhook"})

	rulesCase3 := make(Rules)
	rulesCase3.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})
	rulesCase3.Add(NewPattern("", "*.png"), TargetID{"2", "amqp"})

	rulesCase4 := make(Rules)
	rulesCase4.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})

	testCases := []struct {
		rules          Rules
		objectName     string
		expectedResult TargetIDSet
	}{
		{rulesCase1, "photos.jpg", NewTargetIDSet()},
		{rulesCase2, "photos.jpg", NewTargetIDSet(TargetID{"1", "webhook"})},
		{rulesCase3, "2010/photos.jpg", NewTargetIDSet(TargetID{"1", "webhook"})},
		{rulesCase4, "2000/photos.jpg", NewTargetIDSet()},
	}

	for i, testCase := range testCases {
		result := testCase.rules.Match(testCase.objectName)

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestRulesClone(t *testing.T) {
	rulesCase1 := make(Rules)

	rulesCase2 := make(Rules)
	rulesCase2.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})

	rulesCase3 := make(Rules)
	rulesCase3.Add(NewPattern("", "*.jpg"), TargetID{"1", "webhook"})

	testCases := []struct {
		rules    Rules
		prefix   string
		targetID TargetID
	}{
		{rulesCase1, "2010*", TargetID{"1", "webhook"}},
		{rulesCase2, "2000*", TargetID{"2", "amqp"}},
		{rulesCase3, "2010*", TargetID{"1", "webhook"}},
	}

	for i, testCase := range testCases {
		result := testCase.rules.Clone()

		if !reflect.DeepEqual(result, testCase.rules) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.rules, result)
		}

		result.Add(NewPattern(testCase.prefix, ""), testCase.targetID)
		if reflect.DeepEqual(result, testCase.rules) {
			t.Fatalf("test %v: result: expected: not equal, got: equal", i+1)
		}
	}
}

func TestRulesUnion(t *testing.T) {
	rulesCase1 := make(Rules)
	rules2Case1 := make(Rules)
	expectedResultCase1 := make(Rules)

	rulesCase2 := make(Rules)
	rules2Case2 := make(Rules)
	rules2Case2.Add(NewPattern("*", ""), TargetID{"1", "webhook"})
	expectedResultCase2 := make(Rules)
	expectedResultCase2.Add(NewPattern("*", ""), TargetID{"1", "webhook"})

	rulesCase3 := make(Rules)
	rulesCase3.Add(NewPattern("", "*"), TargetID{"1", "webhook"})
	rules2Case3 := make(Rules)
	expectedResultCase3 := make(Rules)
	expectedResultCase3.Add(NewPattern("", "*"), TargetID{"1", "webhook"})

	rulesCase4 := make(Rules)
	rulesCase4.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})
	rules2Case4 := make(Rules)
	rules2Case4.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})
	expectedResultCase4 := make(Rules)
	expectedResultCase4.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})

	rulesCase5 := make(Rules)
	rulesCase5.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})
	rulesCase5.Add(NewPattern("", "*.png"), TargetID{"2", "amqp"})
	rules2Case5 := make(Rules)
	rules2Case5.Add(NewPattern("*", ""), TargetID{"1", "webhook"})
	expectedResultCase5 := make(Rules)
	expectedResultCase5.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})
	expectedResultCase5.Add(NewPattern("", "*.png"), TargetID{"2", "amqp"})
	expectedResultCase5.Add(NewPattern("*", ""), TargetID{"1", "webhook"})

	testCases := []struct {
		rules          Rules
		rules2         Rules
		expectedResult Rules
	}{
		{rulesCase1, rules2Case1, expectedResultCase1},
		{rulesCase2, rules2Case2, expectedResultCase2},
		{rulesCase3, rules2Case3, expectedResultCase3},
		{rulesCase4, rules2Case4, expectedResultCase4},
		{rulesCase5, rules2Case5, expectedResultCase5},
	}

	for i, testCase := range testCases {
		result := testCase.rules.Union(testCase.rules2)

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestRulesDifference(t *testing.T) {
	rulesCase1 := make(Rules)
	rules2Case1 := make(Rules)
	expectedResultCase1 := make(Rules)

	rulesCase2 := make(Rules)
	rules2Case2 := make(Rules)
	rules2Case2.Add(NewPattern("*", "*"), TargetID{"1", "webhook"})
	expectedResultCase2 := make(Rules)

	rulesCase3 := make(Rules)
	rulesCase3.Add(NewPattern("*", "*"), TargetID{"1", "webhook"})
	rules2Case3 := make(Rules)
	expectedResultCase3 := make(Rules)
	expectedResultCase3.Add(NewPattern("*", "*"), TargetID{"1", "webhook"})

	rulesCase4 := make(Rules)
	rulesCase4.Add(NewPattern("*", "*"), TargetID{"1", "webhook"})
	rules2Case4 := make(Rules)
	rules2Case4.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})
	rules2Case4.Add(NewPattern("", "*.png"), TargetID{"2", "amqp"})
	expectedResultCase4 := make(Rules)
	expectedResultCase4.Add(NewPattern("*", "*"), TargetID{"1", "webhook"})

	rulesCase5 := make(Rules)
	rulesCase5.Add(NewPattern("*", ""), TargetID{"1", "webhook"})
	rulesCase5.Add(NewPattern("", "*"), TargetID{"2", "amqp"})
	rules2Case5 := make(Rules)
	rules2Case5.Add(NewPattern("2010*", ""), TargetID{"1", "webhook"})
	rules2Case5.Add(NewPattern("", "*"), TargetID{"2", "amqp"})
	expectedResultCase5 := make(Rules)
	expectedResultCase5.Add(NewPattern("*", ""), TargetID{"1", "webhook"})

	testCases := []struct {
		rules          Rules
		rules2         Rules
		expectedResult Rules
	}{
		{rulesCase1, rules2Case1, expectedResultCase1},
		{rulesCase2, rules2Case2, expectedResultCase2},
		{rulesCase3, rules2Case3, expectedResultCase3},
		{rulesCase4, rules2Case4, expectedResultCase4},
		{rulesCase5, rules2Case5, expectedResultCase5},
	}

	for i, testCase := range testCases {
		result := testCase.rules.Difference(testCase.rules2)

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}
