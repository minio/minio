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
	"crypto/rand"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/minio/minio/internal/store"
)

type ExampleTarget struct {
	id       TargetID
	sendErr  bool
	closeErr bool
}

func (target ExampleTarget) ID() TargetID {
	return target.id
}

// Save - Sends event directly without persisting.
func (target ExampleTarget) Save(eventData Event) error {
	return target.send(eventData)
}

// Store - Returns a nil store.
func (target ExampleTarget) Store() TargetStore {
	return nil
}

func (target ExampleTarget) send(eventData Event) error {
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

// SendFromStore - interface compatible method does no-op.
func (target *ExampleTarget) SendFromStore(_ store.Key) error {
	return nil
}

func (target ExampleTarget) Close() error {
	if target.closeErr {
		return errors.New("close error")
	}

	return nil
}

func (target ExampleTarget) IsActive() (bool, error) {
	return false, errors.New("not connected to target server/service")
}

// FlushQueueStore - No-Op. Added for interface compatibility
func (target ExampleTarget) FlushQueueStore() error {
	return nil
}

func TestTargetListAdd(t *testing.T) {
	targetListCase1 := NewTargetList(t.Context())

	targetListCase2 := NewTargetList(t.Context())
	if err := targetListCase2.Add(&ExampleTarget{TargetID{"2", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase3 := NewTargetList(t.Context())
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
	targetListCase1 := NewTargetList(t.Context())

	targetListCase2 := NewTargetList(t.Context())
	if err := targetListCase2.Add(&ExampleTarget{TargetID{"2", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase3 := NewTargetList(t.Context())
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

func TestTargetListList(t *testing.T) {
	targetListCase1 := NewTargetList(t.Context())

	targetListCase2 := NewTargetList(t.Context())
	if err := targetListCase2.Add(&ExampleTarget{TargetID{"2", "testcase"}, false, false}); err != nil {
		panic(err)
	}

	targetListCase3 := NewTargetList(t.Context())
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

func TestNewTargetList(t *testing.T) {
	if result := NewTargetList(t.Context()); result == nil {
		t.Fatalf("test: result: expected: <non-nil>, got: <nil>")
	}
}
