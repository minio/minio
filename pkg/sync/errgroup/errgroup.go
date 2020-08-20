/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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

package errgroup

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrTaskIgnored indicates that the corresponding
// has not being executed by the parallel goroutine
// manager because quorum was reached already.
var ErrTaskIgnored = errors.New("task ignored")

type taskStatus struct {
	err      error
	duration time.Duration
}

// A Group is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// A zero Group is valid and does not cancel on error.
type Group struct {
	errs     []error
	doneCh   chan taskStatus
	total    int64
	quitting int64

	minWaitTime time.Duration
	failFactor  int
	quorum      int64
	validErrs   []error
}

// Opts holds configuration of the errgroup go-routine manager
type Opts struct {
	Total       int
	Quorum      int
	ValidErrs   []error
	FailFactor  int
	MinWaitTime time.Duration
}

// New returns a new Group upon Wait() errors are returned collected from all tasks.
func New(opts Opts) *Group {
	errs := make([]error, opts.Total)
	for i := range errs {
		errs[i] = ErrTaskIgnored
	}

	waitTime := 100 * time.Millisecond
	if opts.MinWaitTime > 0 {
		waitTime = opts.MinWaitTime
	}

	return &Group{
		errs:        errs,
		doneCh:      make(chan taskStatus, opts.Total),
		failFactor:  opts.FailFactor,
		minWaitTime: waitTime,
		quorum:      int64(opts.Quorum),
		validErrs:   opts.ValidErrs,
	}
}

// WithNErrs returns a parallel goroutine manager
func WithNErrs(nerrs int) *Group {
	return New(Opts{Total: nerrs})
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the slice of errors from all function calls.
func (g *Group) Wait() (ret []error) {
	/*
		// TODO: remove the following debug code
		debugCh := make(chan struct{})
		defer func() {
			debugCh <- struct{}{}
		}()
		var stack strings.Builder
		stack.Write(debug.Stack())
		go func() {
			select {
			case <-time.NewTimer(5 * time.Second).C:
				fmt.Println(stack.String())
				os.Exit(-1)
			case <-debugCh:
				return
			}
		}()
	*/

	if atomic.LoadInt64(&g.total) == 0 {
		return g.errs
	}

	var done, success, ignored int64
	var maxTaskDuration time.Duration
	var abortTime = 5 * time.Minute

	for {
		var abortTimer <-chan time.Time
		quorum := g.total
		if g.quorum != 0 {
			quorum = g.quorum
		}

		if success >= quorum || ignored >= quorum {
			if g.failFactor > 0 {
				abortTime = maxTaskDuration * time.Duration(g.failFactor)
				if abortTime < g.minWaitTime {
					abortTime = g.minWaitTime
				}
			}
		}

		abortTimer = time.NewTimer(abortTime).C

		select {
		case st := <-g.doneCh:
			done++
			if done == atomic.LoadInt64(&g.total) {
				goto quit
			}
			if st.err == nil {
				success++
			} else {
				for _, e := range g.validErrs {
					if st.err == e {
						ignored++
						break
					}
				}
			}
			if st.duration > maxTaskDuration {
				maxTaskDuration = st.duration
			}
		case <-abortTimer:
			goto quit
		}
	}

quit:
	atomic.StoreInt64(&g.quitting, 1)
	return g.errs
}

// Go calls the given function in a new goroutine.
//
// The first call to return a non-nil error will be
// collected in errs slice and returned by Wait().
func (g *Group) Go(f func() error, index int) {
	if atomic.LoadInt64(&g.quitting) > 0 {
		return
	}
	atomic.AddInt64(&g.total, 1)
	go func() {
		now := time.Now()
		g.errs[index] = f()
		d := time.Since(now)
		g.doneCh <- taskStatus{duration: d, err: g.errs[index]}
	}()
}
