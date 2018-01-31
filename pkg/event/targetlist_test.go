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

import (
	"crypto/rand"
	"errors"
	"reflect"
	"testing"
	"time"
)

type ExampleTarget struct {
	id       TargetID
	sendErr  bool
	closeErr bool
}

func (target ExampleTarget) ID() TargetID {
	return target.id
}

func (target ExampleTarget) Send(eventData Event) error {
	b := make([]byte, 1)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}

	time.Sleep(time.Duration(b[0]) * time.Millisecond)

	if target.sendErr {
		return errors.New("send error")
	}

	return nil
}

func (target ExampleTarget) Close() error {
	if target.closeErr {
		return errors.New("close error")
	}

	return nil
}

func TestTargetListAdd(t *testing.T) {
	targetListCase1 := NewTargetList()

	targetListCase2 := NewTargetList()
	if err := targetListCase2.Add(&ExampleTarget{TargetID{"2", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase3 := NewTargetList()
	if err := targetListCase3.Add(&ExampleTarget{TargetID{"3", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	testCases := []struct {
		targetList     *TargetList
		target         Target
		expectedResult []TargetID
		expectErr      bool
	}{
		{targetListCase1, &ExampleTarget{TargetID{"1", "webhook"}, false, false}, []TargetID{{"1", "webhook"}}, false},
		{targetListCase2, &ExampleTarget{TargetID{"1", "webhook"}, false, false}, []TargetID{{"2", "testcase"}, {"1", "webhook"}}, false},
		{targetListCase3, &ExampleTarget{TargetID{"3", "testcase"}, false, false}, nil, true},
	}

	for i, testCase := range testCases {
		err := testCase.targetList.Add(testCase.target)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			result := testCase.targetList.List()

			if len(result) != len(testCase.expectedResult) {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}

			for _, targetID1 := range result {
				var found bool
				for _, targetID2 := range testCase.expectedResult {
					if reflect.DeepEqual(targetID1, targetID2) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
				}
			}
		}
	}
}

func TestTargetListExists(t *testing.T) {
	targetListCase1 := NewTargetList()

	targetListCase2 := NewTargetList()
	if err := targetListCase2.Add(&ExampleTarget{TargetID{"2", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase3 := NewTargetList()
	if err := targetListCase3.Add(&ExampleTarget{TargetID{"3", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	testCases := []struct {
		targetList     *TargetList
		targetID       TargetID
		expectedResult bool
	}{
		{targetListCase1, TargetID{"1", "webhook"}, false},
		{targetListCase2, TargetID{"1", "webhook"}, false},
		{targetListCase3, TargetID{"3", "testcase"}, true},
	}

	for i, testCase := range testCases {
		result := testCase.targetList.Exists(testCase.targetID)

		if result != testCase.expectedResult {
			t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestTargetListRemove(t *testing.T) {
	targetListCase1 := NewTargetList()

	targetListCase2 := NewTargetList()
	if err := targetListCase2.Add(&ExampleTarget{TargetID{"2", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase3 := NewTargetList()
	if err := targetListCase3.Add(&ExampleTarget{TargetID{"3", "testcase"}, false, true}); err != nil {
		panic(err)
	}

	testCases := []struct {
		targetList *TargetList
		targetID   TargetID
		expectErr  bool
	}{
		{targetListCase1, TargetID{"1", "webhook"}, false},
		{targetListCase2, TargetID{"1", "webhook"}, false},
		{targetListCase3, TargetID{"3", "testcase"}, true},
	}

	for i, testCase := range testCases {
		errors := testCase.targetList.Remove(testCase.targetID)
		err := errors[testCase.targetID]
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestTargetListList(t *testing.T) {
	targetListCase1 := NewTargetList()

	targetListCase2 := NewTargetList()
	if err := targetListCase2.Add(&ExampleTarget{TargetID{"2", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase3 := NewTargetList()
	if err := targetListCase3.Add(&ExampleTarget{TargetID{"3", "testcase"}, false, false}); err != nil {
		panic(err)
	}
	if err := targetListCase3.Add(&ExampleTarget{TargetID{"1", "webhook"}, false, false}); err != nil {
		panic(err)
	}

	testCases := []struct {
		targetList     *TargetList
		expectedResult []TargetID
	}{
		{targetListCase1, []TargetID{}},
		{targetListCase2, []TargetID{{"2", "testcase"}}},
		{targetListCase3, []TargetID{{"3", "testcase"}, {"1", "webhook"}}},
	}

	for i, testCase := range testCases {
		result := testCase.targetList.List()

		if len(result) != len(testCase.expectedResult) {
			t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}

		for _, targetID1 := range result {
			var found bool
			for _, targetID2 := range testCase.expectedResult {
				if reflect.DeepEqual(targetID1, targetID2) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("test %v: data: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestTargetListSend(t *testing.T) {
	targetListCase1 := NewTargetList()

	targetListCase2 := NewTargetList()
	if err := targetListCase2.Add(&ExampleTarget{TargetID{"2", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase3 := NewTargetList()
	if err := targetListCase3.Add(&ExampleTarget{TargetID{"3", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase4 := NewTargetList()
	if err := targetListCase4.Add(&ExampleTarget{TargetID{"4", "testcase"}, true, false}); err != nil {
		panic(err)
	}

	testCases := []struct {
		targetList *TargetList
		targetID   TargetID
		expectErr  bool
	}{
		{targetListCase1, TargetID{"1", "webhook"}, false},
		{targetListCase2, TargetID{"1", "non-existent"}, false},
		{targetListCase3, TargetID{"3", "testcase"}, false},
		{targetListCase4, TargetID{"4", "testcase"}, true},
	}

	for i, testCase := range testCases {
		errors := testCase.targetList.Send(Event{}, testCase.targetID)
		err := errors[testCase.targetID]
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestNewTargetList(t *testing.T) {
	if result := NewTargetList(); result == nil {
		t.Fatalf("test: result: expected: <non-nil>, got: <nil>")
	}
}
