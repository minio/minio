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
	"context"
	"errors"
	"sync"
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
// A zero Group can be used if errors should not be tracked.
type Group struct {
	firstErr  int64 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	wg        sync.WaitGroup
	bucket    chan struct{}
	errs      []error
	cancel    context.CancelFunc
	ctxCancel <-chan struct{} // nil if no context.
	ctxErr    func() error

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
	return &Group{errs: make([]error, nerrs), firstErr: -1}
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

	defer func() {
		if g.cancel != nil {
			g.cancel()
		}
	}()

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

// WaitErr blocks until all function calls from the Go method have returned, then
// returns the first error returned.
func (g *Group) WaitErr() error {
	g.Wait()
	if g.firstErr >= 0 && len(g.errs) > int(g.firstErr) {
		// len(g.errs) > int(g.firstErr) is for then used uninitialized.
		return g.errs[g.firstErr]
	}
	return nil
}

// WithConcurrency allows to limit the concurrency of the group.
// This must be called before starting any async processes.
// There is no order to which functions are allowed to run.
// If n <= 0 no concurrency limits are enforced.
// g is modified and returned as well.
func (g *Group) WithConcurrency(n int) *Group {
	if n <= 0 {
		g.bucket = nil
		return g
	}

	// Fill bucket with tokens
	g.bucket = make(chan struct{}, n)
	for i := 0; i < n; i++ {
		g.bucket <- struct{}{}
	}
	return g
}

// WithCancelOnError will return a context that is canceled
// as soon as an error occurs.
// The returned CancelFunc must always be called similar to context.WithCancel.
// If the supplied context is canceled any goroutines waiting for execution are also canceled.
func (g *Group) WithCancelOnError(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, g.cancel = context.WithCancel(ctx)
	g.ctxCancel = ctx.Done()
	g.ctxErr = ctx.Err
	return ctx, g.cancel
}

// Go calls the given function in a new goroutine.
//
// The errors will be collected in errs slice and returned by Wait().
func (g *Group) Go(f func() error, index int) {
	if atomic.LoadInt64(&g.quitting) > 0 {
		return
	}
	atomic.AddInt64(&g.total, 1)

	go func() {
		if g.bucket != nil {
			// Wait for token
			select {
			case <-g.bucket:
				defer func() {
					// Put back token..
					g.bucket <- struct{}{}
				}()
			case <-g.ctxCancel:
				if len(g.errs) > index {
					atomic.CompareAndSwapInt64(&g.firstErr, -1, int64(index))
					g.errs[index] = g.ctxErr()
				}
				return
			}
		}
		now := time.Now()
		err := f()
		d := time.Since(now)
		if err != nil {
			if len(g.errs) > index {
				atomic.CompareAndSwapInt64(&g.firstErr, -1, int64(index))
				g.errs[index] = err
			}
			if g.cancel != nil {
				g.cancel()
			}
		}
		g.doneCh <- taskStatus{duration: d, err: g.errs[index]}

	}()
}
