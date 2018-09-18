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

package time

import (
	"testing"
	"time"
)

func TestExponentTicker(t *testing.T) {
	testCases := []struct {
		min time.Duration
		max time.Duration
	}{
		{10 * time.Millisecond, 1 * time.Second},
		// ExponentTicker works exactly like time.Ticker here, but not recommended for practical use.
		{10 * time.Millisecond, 10 * time.Millisecond},
	}

	for i, testCase := range testCases {
		count := 10
		minDelta := testCase.min * time.Duration(count)
		maxDelta := testCase.max * time.Duration(count)
		maxSlop := 2 * testCase.max

		ticker := NewExponentTicker(testCase.min, testCase.max)
		t1 := time.Now()
		for j := 0; j < count; j++ {
			<-ticker.C
		}
		ticker.Stop()
		delta := time.Now().Sub(t1)

		if (delta < minDelta) || (delta > maxDelta+maxSlop) {
			t.Fatalf("case %v: got: %v, expected: between %v and %v", i+1, delta, minDelta, maxDelta+maxSlop)
		}
	}
}

func TestExponentTickerStopWithDirectInitialization(t *testing.T) {
	c := make(chan time.Time)
	ticker := &ExponentTicker{C: c}
	ticker.Stop()
}

func TestNewExponentTickerPanics(t *testing.T) {
	testCases := []struct {
		min time.Duration
		max time.Duration
	}{
		{-1, 10},
		{10, 0},
		{10, 9},
	}

	for i, testCase := range testCases {
		func() {
			defer func() {
				if err := recover(); err == nil {
					t.Fatalf("case %v: NewExponentTicker(%v, %v) should have panicked", i+1, testCase.min, testCase.max)
				}
			}()
			NewExponentTicker(testCase.min, testCase.max)
		}()
	}
}
