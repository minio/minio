/*
 * Minio Cloud Storage, (C) 2020 Minio, Inc.
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

package retry

import (
	"context"
	"testing"
	"time"
)

// Tests for retry timer.
func TestRetryTimerSimple(t *testing.T) {
	retryCtx, cancel := context.WithCancel(context.Background())
	attemptCh := NewTimer(retryCtx)
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
	retryCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No jitter
	attemptCh := NewTimerWithJitter(retryCtx, time.Millisecond, 5*time.Millisecond, NoJitter)
	i := <-attemptCh
	if i != 0 {
		cancel()
		t.Fatalf("Invalid attempt counter returned should be 0, found %d instead", i)
	}
	// Loop through the maximum possible attempt.
	for i = range attemptCh {
		if i == 30 {
			cancel()
		}
	}
	_, ok := <-attemptCh
	if ok {
		t.Fatal("Attempt counter should be closed")
	}
}

// Test retry time with Jitter greater than MaxJitter.
func TestRetryTimerWithJitter(t *testing.T) {
	retryCtx, cancel := context.WithCancel(context.Background())
	// Jitter will be set back to 1.0
	attemptCh := NewTimerWithJitter(retryCtx, time.Second, 30*time.Second, 2.0)
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
