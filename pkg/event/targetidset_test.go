/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
