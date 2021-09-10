// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package errgroup

import (
	"context"
	"sync"
	"sync/atomic"
)

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
}

// WithNErrs returns a new Group with length of errs slice upto nerrs,
// upon Wait() errors are returned collected from all tasks.
func WithNErrs(nerrs int) *Group {
	return &Group{errs: make([]error, nerrs), firstErr: -1}
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the slice of errors from all function calls.
func (g *Group) Wait() []error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}
	return g.errs
}

// WaitErr blocks until all function calls from the Go method have returned, then
// returns the first error returned.
func (g *Group) WaitErr() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}
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
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
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
		if err := f(); err != nil {
			if len(g.errs) > index {
				atomic.CompareAndSwapInt64(&g.firstErr, -1, int64(index))
				g.errs[index] = err
			}
			if g.cancel != nil {
				g.cancel()
			}
		}
	}()
}
