// Copyright (c) 2015-2023 MinIO, Inc.
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

func TestTargetIDSetClone(t *testing.T) {
	testCases := []struct {
		set           TargetIDSet
		targetIDToAdd TargetID
	}{
		{NewTargetIDSet(), TargetID{"1", "webhook"}},
		{NewTargetIDSet(TargetID{"1", "webhook"}), TargetID{"2", "webhook"}},
		{NewTargetIDSet(TargetID{"1", "webhook"}, TargetID{"2", "amqp"}), TargetID{"2", "webhook"}},
	}

	for i, testCase := range testCases {
		result := testCase.set.Clone()

		if !reflect.DeepEqual(result, testCase.set) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.set, result)
		}

		result.add(testCase.targetIDToAdd)
		if reflect.DeepEqual(result, testCase.set) {
			t.Fatalf("test %v: result: expected: not equal, got: equal", i+1)
		}
	}
}

func TestTargetIDSetUnion(t *testing.T) {
	testCases := []struct {
		set            TargetIDSet
		setToAdd       TargetIDSet
		expectedResult TargetIDSet
	}{
		{NewTargetIDSet(), NewTargetIDSet(), NewTargetIDSet()},
		{NewTargetIDSet(), NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet(TargetID{"1", "webhook"})},
		{NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet(), NewTargetIDSet(TargetID{"1", "webhook"})},
		{NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet(TargetID{"2", "amqp"}), NewTargetIDSet(TargetID{"1", "webhook"}, TargetID{"2", "amqp"})},
		{NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet(TargetID{"1", "webhook"})},
	}

	for i, testCase := range testCases {
		result := testCase.set.Union(testCase.setToAdd)

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestTargetIDSetDifference(t *testing.T) {
	testCases := []struct {
		set            TargetIDSet
		setToRemove    TargetIDSet
		expectedResult TargetIDSet
	}{
		{NewTargetIDSet(), NewTargetIDSet(), NewTargetIDSet()},
		{NewTargetIDSet(), NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet()},
		{NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet(), NewTargetIDSet(TargetID{"1", "webhook"})},
		{NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet(TargetID{"2", "amqp"}), NewTargetIDSet(TargetID{"1", "webhook"})},
		{NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet(TargetID{"1", "webhook"}), NewTargetIDSet()},
	}

	for i, testCase := range testCases {
		result := testCase.set.Difference(testCase.setToRemove)

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNewTargetIDSet(t *testing.T) {
	testCases := []struct {
		targetIDs      []TargetID
		expectedResult TargetIDSet
	}{
		{[]TargetID{}, NewTargetIDSet()},
		{[]TargetID{{"1", "webhook"}}, NewTargetIDSet(TargetID{"1", "webhook"})},
		{[]TargetID{{"1", "webhook"}, {"2", "amqp"}}, NewTargetIDSet(TargetID{"1", "webhook"}, TargetID{"2", "amqp"})},
	}

	for i, testCase := range testCases {
		result := NewTargetIDSet(testCase.targetIDs...)

		if !reflect.DeepEqual(testCase.expectedResult, result) {
			t.Fatalf("test %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}
