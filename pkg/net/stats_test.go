/*
* Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package net

import (
	"testing"
)

func TestSlidingWindowAtStart(t *testing.T) {
	circularIndex = 0

	// when the server starts, stat request behavior will
	// be based on circular index being 0
	expectedOneMinIndex := 15
	obtainedOneMinIndex := minuteSelector(windowSize, 1)
	if expectedOneMinIndex != obtainedOneMinIndex {
		t.Fatalf("invalid minute selector: index of value 1 minute ago should be %d, not %d", expectedOneMinIndex, obtainedOneMinIndex)
	}

	expectedFiveMinIndex := 11
	obtainedFiveMinIndex := minuteSelector(windowSize, 5)
	if expectedFiveMinIndex != obtainedFiveMinIndex {
		t.Fatalf("invalid minute selector: index of value 5 minute ago should be %d, not %d", expectedFiveMinIndex, obtainedFiveMinIndex)
	}

	expectedFifteenMinIndex := 1
	obtainedFifteenMinIndex := minuteSelector(windowSize, 15)
	if expectedFifteenMinIndex != obtainedFifteenMinIndex {
		t.Fatalf("invalid minute selector: index of value 15 minute ago should be %d, not %d", expectedFifteenMinIndex, obtainedFifteenMinIndex)
	}
}

func TestSlidingWindowAtfterOneMin(t *testing.T) {
	circularIndex = 1

	expectedOneMinIndex := 0
	obtainedOneMinIndex := minuteSelector(windowSize, 1)
	if expectedOneMinIndex != obtainedOneMinIndex {
		t.Fatalf("invalid minute selector: index of value 1 minute ago should be %d, not %d", expectedOneMinIndex, obtainedOneMinIndex)
	}

	expectedFiveMinIndex := 12
	obtainedFiveMinIndex := minuteSelector(windowSize, 5)
	if expectedFiveMinIndex != obtainedFiveMinIndex {
		t.Fatalf("invalid minute selector: index of value 5 minute ago should be %d, not %d", expectedFiveMinIndex, obtainedFiveMinIndex)
	}

	expectedFifteenMinIndex := 2
	obtainedFifteenMinIndex := minuteSelector(windowSize, 15)
	if expectedFifteenMinIndex != obtainedFifteenMinIndex {
		t.Fatalf("invalid minute selector: index of value 15 minute ago should be %d, not %d", expectedFifteenMinIndex, obtainedFifteenMinIndex)
	}
}

func TestSlidingWindowAfterFiveMins(t *testing.T) {
	circularIndex = 5

	expectedOneMinIndex := 4
	obtainedOneMinIndex := minuteSelector(windowSize, 1)
	if expectedOneMinIndex != obtainedOneMinIndex {
		t.Fatalf("invalid minute selector: index of value 1 minute ago should be %d, not %d", expectedOneMinIndex, obtainedOneMinIndex)
	}

	expectedFiveMinIndex := 0
	obtainedFiveMinIndex := minuteSelector(windowSize, 5)
	if expectedFiveMinIndex != obtainedFiveMinIndex {
		t.Fatalf("invalid minute selector: index of value 5 minute ago should be %d, not %d", expectedFiveMinIndex, obtainedFiveMinIndex)
	}

	expectedFifteenMinIndex := 6
	obtainedFifteenMinIndex := minuteSelector(windowSize, 15)
	if expectedFifteenMinIndex != obtainedFifteenMinIndex {
		t.Fatalf("invalid minute selector: index of value 15 minute ago should be %d, not %d", expectedFifteenMinIndex, obtainedFifteenMinIndex)
	}
}

func TestSlidingWindowAtfterFifteenMins(t *testing.T) {
	circularIndex = 15

	expectedOneMinIndex := 14
	obtainedOneMinIndex := minuteSelector(windowSize, 1)
	if expectedOneMinIndex != obtainedOneMinIndex {
		t.Fatalf("invalid minute selector: index of value 1 minute ago should be %d, not %d", expectedOneMinIndex, obtainedOneMinIndex)
	}

	expectedFiveMinIndex := 10
	obtainedFiveMinIndex := minuteSelector(windowSize, 5)
	if expectedFiveMinIndex != obtainedFiveMinIndex {
		t.Fatalf("invalid minute selector: index of value 5 minute ago should be %d, not %d", expectedFiveMinIndex, obtainedFiveMinIndex)
	}

	expectedFifteenMinIndex := 0
	obtainedFifteenMinIndex := minuteSelector(windowSize, 15)
	if expectedFifteenMinIndex != obtainedFifteenMinIndex {
		t.Fatalf("invalid minute selector: index of value 15 minute ago should be %d, not %d", expectedFifteenMinIndex, obtainedFifteenMinIndex)
	}
}
