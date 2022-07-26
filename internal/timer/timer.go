package timer

import "time"

// Timer wrap time.Timer , change Stop performance.
type Timer struct {
	*time.Timer
}

// NewTimer creates a new Timer that will send
// the current time on its channel after at least duration d.
func NewTimer(d time.Duration) *Timer {
	return &Timer{time.NewTimer(d)}
}

// Stop prevents the Timer from firing.
// It returns true if the call stops the timer, false if the timer has already
// expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding
// incorrectly.
//
// To ensure the channel is empty after a call to Stop, check the
// return value and drain the channel.
// For example, assuming the program has not received from t.C already:
//
// 	if !t.Stop() {
// 		<-t.C
// 	}
//
// This cannot be done concurrent to other receives from the Timer's
// channel or other calls to the Timer's Stop method.
//
// For a timer created with AfterFunc(d, f), if t.Stop returns false, then the timer
// has already expired and the function f has been started in its own goroutine;
// Stop does not wait for f to complete before returning.
// If the caller needs to know whether f is completed, it must coordinate
// with f explicitly.
func (t *Timer) Stop() bool {
	stop := t.Timer.Stop()
	if !stop {
		<-t.C
	}
	return stop
}
