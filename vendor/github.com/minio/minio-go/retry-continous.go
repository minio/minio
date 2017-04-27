package minio

import "time"

// newRetryTimerContinous creates a timer with exponentially increasing delays forever.
func (c Client) newRetryTimerContinous(unit time.Duration, cap time.Duration, jitter float64, doneCh chan struct{}) <-chan int {
	attemptCh := make(chan int)

	// normalize jitter to the range [0, 1.0]
	if jitter < NoJitter {
		jitter = NoJitter
	}
	if jitter > MaxJitter {
		jitter = MaxJitter
	}

	// computes the exponential backoff duration according to
	// https://www.awsarchitectureblog.com/2015/03/backoff.html
	exponentialBackoffWait := func(attempt int) time.Duration {
		// 1<<uint(attempt) below could overflow, so limit the value of attempt
		maxAttempt := 30
		if attempt > maxAttempt {
			attempt = maxAttempt
		}
		//sleep = random_between(0, min(cap, base * 2 ** attempt))
		sleep := unit * time.Duration(1<<uint(attempt))
		if sleep > cap {
			sleep = cap
		}
		if jitter != NoJitter {
			sleep -= time.Duration(c.random.Float64() * float64(sleep) * jitter)
		}
		return sleep
	}

	go func() {
		defer close(attemptCh)
		var nextBackoff int
		for {
			select {
			// Attempts starts.
			case attemptCh <- nextBackoff:
				nextBackoff++
			case <-doneCh:
				// Stop the routine.
				return
			}
			time.Sleep(exponentialBackoffWait(nextBackoff))
		}
	}()
	return attemptCh
}
