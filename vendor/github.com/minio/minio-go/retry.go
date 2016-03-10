/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015, 2016 Minio, Inc.
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

package minio

import (
	"math/rand"
	"net"
	"time"
)

// MaxRetry is the maximum number of retries before stopping.
var MaxRetry = 5

// MaxJitter will randomize over the full exponential backoff time
const MaxJitter = 1.0

// NoJitter disables the use of jitter for randomizing the exponential backoff time
const NoJitter = 0.0

// newRetryTimer creates a timer with exponentially increasing delays
// until the maximum retry attempts are reached.
func newRetryTimer(maxRetry int, unit time.Duration, cap time.Duration, jitter float64) <-chan int {
	attemptCh := make(chan int)

	// Seed random function with current unix nano time.
	rand.Seed(time.Now().UTC().UnixNano())

	go func() {
		defer close(attemptCh)
		for i := 0; i < maxRetry; i++ {
			attemptCh <- i + 1 // Attempts start from 1.
			// Grow the interval at an exponential rate,
			// starting at unit and capping at cap
			time.Sleep(exponentialBackoffWait(unit, i, cap, jitter))
		}
	}()
	return attemptCh
}

// isNetErrorRetryable - is network error retryable.
func isNetErrorRetryable(err error) bool {
	switch err.(type) {
	case *net.DNSError, *net.OpError, net.UnknownNetworkError:
		return true
	}
	return false
}

// computes the exponential backoff duration according to
// https://www.awsarchitectureblog.com/2015/03/backoff.html
func exponentialBackoffWait(base time.Duration, attempt int, cap time.Duration, jitter float64) time.Duration {
	// normalize jitter to the range [0, 1.0]
	if jitter < NoJitter {
		jitter = NoJitter
	}
	if jitter > MaxJitter {
		jitter = MaxJitter
	}
	//sleep = random_between(0, min(cap, base * 2 ** attempt))
	sleep := base * time.Duration(1<<uint(attempt))
	if sleep > cap {
		sleep = cap
	}
	if jitter != NoJitter {
		sleep -= time.Duration(rand.Float64() * float64(sleep) * jitter)
	}
	return sleep
}

// isS3CodeRetryable - is s3 error code retryable.
func isS3CodeRetryable(s3Code string) bool {
	switch s3Code {
	case "RequestError", "RequestTimeout", "Throttling", "ThrottlingException":
		fallthrough
	case "RequestLimitExceeded", "RequestThrottled", "InternalError":
		fallthrough
	case "ExpiredToken", "ExpiredTokenException":
		return true
	}
	return false
}
