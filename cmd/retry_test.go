/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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

package cmd

import (
	"testing"
	"time"
)

// Tests for retry timer.
func TestRetryTimerSimple(t *testing.T) {
	doneCh := make(chan struct{})
	attemptCh := newRetryTimerSimple(doneCh)
	i := <-attemptCh
	if i != 0 {
		close(doneCh)
		t.Fatalf("Invalid attempt counter returned should be 0, found %d instead", i)
	}
	i = <-attemptCh
	if i <= 0 {
		close(doneCh)
		t.Fatalf("Invalid attempt counter returned should be greater than 0, found %d instead", i)
	}
	close(doneCh)
	_, ok := <-attemptCh
	if ok {
		t.Fatal("Attempt counter should be closed")
	}
}

// Test retry time with no jitter.
func TestRetryTimerWithNoJitter(t *testing.T) {
	doneCh := make(chan struct{})
	// No jitter
	attemptCh := newRetryTimerWithJitter(time.Millisecond, 5*time.Millisecond, NoJitter, doneCh)
	i := <-attemptCh
	if i != 0 {
		close(doneCh)
		t.Fatalf("Invalid attempt counter returned should be 0, found %d instead", i)
	}
	// Loop through the maximum possible attempt.
	for i = range attemptCh {
		if i == 30 {
			close(doneCh)
		}
	}
	_, ok := <-attemptCh
	if ok {
		t.Fatal("Attempt counter should be closed")
	}
}

// Test retry time with Jitter greater than MaxJitter.
func TestRetryTimerWithJitter(t *testing.T) {
	doneCh := make(chan struct{})
	// Jitter will be set back to 1.0
	attemptCh := newRetryTimerWithJitter(defaultRetryUnit, defaultRetryCap, 2.0, doneCh)
	i := <-attemptCh
	if i != 0 {
		close(doneCh)
		t.Fatalf("Invalid attempt counter returned should be 0, found %d instead", i)
	}
	close(doneCh)
	_, ok := <-attemptCh
	if ok {
		t.Fatal("Attempt counter should be closed")
	}
}
