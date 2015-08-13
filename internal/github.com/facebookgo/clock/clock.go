package clock

import (
	"runtime"
	"sort"
	"sync"
	"time"
)

// Clock represents an interface to the functions in the standard library time
// package. Two implementations are available in the clock package. The first
// is a real-time clock which simply wraps the time package's functions. The
// second is a mock clock which will only make forward progress when
// programmatically adjusted.
type Clock interface {
	After(d time.Duration) <-chan time.Time
	AfterFunc(d time.Duration, f func()) *Timer
	Now() time.Time
	Sleep(d time.Duration)
	Tick(d time.Duration) <-chan time.Time
	Ticker(d time.Duration) *Ticker
	Timer(d time.Duration) *Timer
}

// New returns an instance of a real-time clock.
func New() Clock {
	return &clock{}
}

// clock implements a real-time clock by simply wrapping the time package functions.
type clock struct{}

func (c *clock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (c *clock) AfterFunc(d time.Duration, f func()) *Timer {
	return &Timer{timer: time.AfterFunc(d, f)}
}

func (c *clock) Now() time.Time { return time.Now() }

func (c *clock) Sleep(d time.Duration) { time.Sleep(d) }

func (c *clock) Tick(d time.Duration) <-chan time.Time { return time.Tick(d) }

func (c *clock) Ticker(d time.Duration) *Ticker {
	t := time.NewTicker(d)
	return &Ticker{C: t.C, ticker: t}
}

func (c *clock) Timer(d time.Duration) *Timer {
	t := time.NewTimer(d)
	return &Timer{C: t.C, timer: t}
}

// Mock represents a mock clock that only moves forward programmically.
// It can be preferable to a real-time clock when testing time-based functionality.
type Mock struct {
	mu     sync.Mutex
	now    time.Time   // current time
	timers clockTimers // tickers & timers

	calls      Calls
	waiting    []waiting
	callsMutex sync.Mutex
}

// NewMock returns an instance of a mock clock.
// The current time of the mock clock on initialization is the Unix epoch.
func NewMock() *Mock {
	return &Mock{now: time.Unix(0, 0)}
}

// Add moves the current time of the mock clock forward by the duration.
// This should only be called from a single goroutine at a time.
func (m *Mock) Add(d time.Duration) {
	// Calculate the final current time.
	t := m.now.Add(d)

	// Continue to execute timers until there are no more before the new time.
	for {
		if !m.runNextTimer(t) {
			break
		}
	}

	// Ensure that we end with the new time.
	m.mu.Lock()
	m.now = t
	m.mu.Unlock()

	// Give a small buffer to make sure the other goroutines get handled.
	gosched()
}

// runNextTimer executes the next timer in chronological order and moves the
// current time to the timer's next tick time. The next time is not executed if
// it's next time if after the max time. Returns true if a timer is executed.
func (m *Mock) runNextTimer(max time.Time) bool {
	m.mu.Lock()

	// Sort timers by time.
	sort.Sort(m.timers)

	// If we have no more timers then exit.
	if len(m.timers) == 0 {
		m.mu.Unlock()
		return false
	}

	// Retrieve next timer. Exit if next tick is after new time.
	t := m.timers[0]
	if t.Next().After(max) {
		m.mu.Unlock()
		return false
	}

	// Move "now" forward and unlock clock.
	m.now = t.Next()
	m.mu.Unlock()

	// Execute timer.
	t.Tick(m.now)
	return true
}

// After waits for the duration to elapse and then sends the current time on the returned channel.
func (m *Mock) After(d time.Duration) <-chan time.Time {
	defer m.inc(&m.calls.After)
	return m.Timer(d).C
}

// AfterFunc waits for the duration to elapse and then executes a function.
// A Timer is returned that can be stopped.
func (m *Mock) AfterFunc(d time.Duration, f func()) *Timer {
	defer m.inc(&m.calls.AfterFunc)
	t := m.Timer(d)
	t.C = nil
	t.fn = f
	return t
}

// Now returns the current wall time on the mock clock.
func (m *Mock) Now() time.Time {
	defer m.inc(&m.calls.Now)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Sleep pauses the goroutine for the given duration on the mock clock.
// The clock must be moved forward in a separate goroutine.
func (m *Mock) Sleep(d time.Duration) {
	defer m.inc(&m.calls.Sleep)
	<-m.After(d)
}

// Tick is a convenience function for Ticker().
// It will return a ticker channel that cannot be stopped.
func (m *Mock) Tick(d time.Duration) <-chan time.Time {
	defer m.inc(&m.calls.Tick)
	return m.Ticker(d).C
}

// Ticker creates a new instance of Ticker.
func (m *Mock) Ticker(d time.Duration) *Ticker {
	defer m.inc(&m.calls.Ticker)
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan time.Time)
	t := &Ticker{
		C:    ch,
		c:    ch,
		mock: m,
		d:    d,
		next: m.now.Add(d),
	}
	m.timers = append(m.timers, (*internalTicker)(t))
	return t
}

// Timer creates a new instance of Timer.
func (m *Mock) Timer(d time.Duration) *Timer {
	defer m.inc(&m.calls.Timer)
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan time.Time)
	t := &Timer{
		C:    ch,
		c:    ch,
		mock: m,
		next: m.now.Add(d),
	}
	m.timers = append(m.timers, (*internalTimer)(t))
	return t
}

func (m *Mock) removeClockTimer(t clockTimer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, timer := range m.timers {
		if timer == t {
			copy(m.timers[i:], m.timers[i+1:])
			m.timers[len(m.timers)-1] = nil
			m.timers = m.timers[:len(m.timers)-1]
			break
		}
	}
	sort.Sort(m.timers)
}

func (m *Mock) inc(addr *uint32) {
	m.callsMutex.Lock()
	defer m.callsMutex.Unlock()
	*addr++
	var newWaiting []waiting
	for _, w := range m.waiting {
		if m.calls.atLeast(w.expected) {
			close(w.done)
			continue
		}
		newWaiting = append(newWaiting, w)
	}
	m.waiting = newWaiting
}

// Wait waits for at least the relevant calls before returning. The expected
// Calls are always over the lifetime of the Mock. Values in the Calls struct
// are used as the minimum number of calls, this allows you to wait for only
// the calls you care about.
func (m *Mock) Wait(s Calls) {
	m.callsMutex.Lock()
	if m.calls.atLeast(s) {
		m.callsMutex.Unlock()
		return
	}
	done := make(chan struct{})
	m.waiting = append(m.waiting, waiting{expected: s, done: done})
	m.callsMutex.Unlock()
	<-done
}

// clockTimer represents an object with an associated start time.
type clockTimer interface {
	Next() time.Time
	Tick(time.Time)
}

// clockTimers represents a list of sortable timers.
type clockTimers []clockTimer

func (a clockTimers) Len() int           { return len(a) }
func (a clockTimers) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a clockTimers) Less(i, j int) bool { return a[i].Next().Before(a[j].Next()) }

// Timer represents a single event.
// The current time will be sent on C, unless the timer was created by AfterFunc.
type Timer struct {
	C     <-chan time.Time
	c     chan time.Time
	timer *time.Timer // realtime impl, if set
	next  time.Time   // next tick time
	mock  *Mock       // mock clock, if set
	fn    func()      // AfterFunc function, if set
}

// Stop turns off the ticker.
func (t *Timer) Stop() {
	if t.timer != nil {
		t.timer.Stop()
	} else {
		t.mock.removeClockTimer((*internalTimer)(t))
	}
}

type internalTimer Timer

func (t *internalTimer) Next() time.Time { return t.next }
func (t *internalTimer) Tick(now time.Time) {
	if t.fn != nil {
		t.fn()
	} else {
		t.c <- now
	}
	t.mock.removeClockTimer((*internalTimer)(t))
	gosched()
}

// Ticker holds a channel that receives "ticks" at regular intervals.
type Ticker struct {
	C      <-chan time.Time
	c      chan time.Time
	ticker *time.Ticker  // realtime impl, if set
	next   time.Time     // next tick time
	mock   *Mock         // mock clock, if set
	d      time.Duration // time between ticks
}

// Stop turns off the ticker.
func (t *Ticker) Stop() {
	if t.ticker != nil {
		t.ticker.Stop()
	} else {
		t.mock.removeClockTimer((*internalTicker)(t))
	}
}

type internalTicker Ticker

func (t *internalTicker) Next() time.Time { return t.next }
func (t *internalTicker) Tick(now time.Time) {
	select {
	case t.c <- now:
	case <-time.After(1 * time.Millisecond):
	}
	t.next = now.Add(t.d)
	gosched()
}

// Sleep momentarily so that other goroutines can process.
func gosched() { runtime.Gosched() }

// Calls keeps track of the count of calls for each of the methods on the Clock
// interface.
type Calls struct {
	After     uint32
	AfterFunc uint32
	Now       uint32
	Sleep     uint32
	Tick      uint32
	Ticker    uint32
	Timer     uint32
}

// atLeast returns true if at least the number of calls in o have been made.
func (c Calls) atLeast(o Calls) bool {
	if c.After < o.After {
		return false
	}
	if c.AfterFunc < o.AfterFunc {
		return false
	}
	if c.Now < o.Now {
		return false
	}
	if c.Sleep < o.Sleep {
		return false
	}
	if c.Tick < o.Tick {
		return false
	}
	if c.Ticker < o.Ticker {
		return false
	}
	if c.Timer < o.Timer {
		return false
	}
	return true
}

type waiting struct {
	expected Calls
	done     chan struct{}
}
