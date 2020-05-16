/*
 * Minio Cloud Storage, (C) 2016-2020 Minio, Inc.
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
	"context"
	"testing"
	"time"
)

// Tests for retry timer.
func TestRetryTimerSimple(t *testing.T) {
	rctx, cancel := context.WithCancel(context.Background())
	attemptCh := newRetryTimerSimple(rctx)
	i := <-attemptCh
	if i != 0 {
		cancel()
		t.Fatalf("Invalid attempt counter returned should be 0, found %d instead", i)
	}
	i = <-attemptCh
	if i <= 0 {
		cancel()
		t.Fatalf("Invalid attempt counter returned should be greater than 0, found %d instead", i)
	}
	cancel()
	_, ok := <-attemptCh
	if ok {
		t.Fatal("Attempt counter should be closed")
	}
}

// Test retry time with no jitter.
func TestRetryTimerWithNoJitter(t *testing.T) {
	rctx, cancel := context.WithCancel(context.Background())

	// No jitter
	attemptCh := newRetryTimerWithJitter(rctx, time.Millisecond, 5*time.Millisecond, NoJitter)
	i := <-attemptCh
	if i != 0 {
		cancel()
		t.Fatalf("Invalid attempt counter returned should be 0, found %d instead", i)
	}
	// Loop through the maximum possible attempt.
	for i = range attemptCh {
		if i == 30 {
			break
		}
	}

	cancel()
	_, ok := <-attemptCh
	if ok {
		t.Fatal("Attempt counter should be closed")
	}
}

// Test retry time with Jitter greater than MaxJitter.
func TestRetryTimerWithJitter(t *testing.T) {
	rctx, cancel := context.WithCancel(context.Background())

	// Jitter will be set back to 1.0
	attemptCh := newRetryTimerWithJitter(rctx, time.Second, 30*time.Second, 2.0)
	i := <-attemptCh
	if i != 0 {
		cancel()
		t.Fatalf("Invalid attempt counter returned should be 0, found %d instead", i)
	}
	cancel()
	_, ok := <-attemptCh
	if ok {
		t.Fatal("Attempt counter should be closed")
	}
}
