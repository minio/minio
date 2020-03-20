/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

package ctxgroup

import (
	"context"
	"sync"
)

// A Group is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// A zero Group is valid and does not cancel on error.
type Group struct {
	count  int
	ctx    context.Context
	cancel context.CancelFunc

	wg    sync.WaitGroup
	tasks chan int
}

// WithNErrs returns a new Group with length of errs slice upto nerrs,
// upon Wait() errors are returned collected from all tasks.
func WithNTasks(ctx context.Context, n int) *Group {
	subCtx, cancel := context.WithCancel(ctx)
	return &Group{
		ctx:    subCtx,
		cancel: cancel,
		count:  n,

		tasks: make(chan int, n),
	}
}

// Wait blocks until all function calls from the Go method have returned
func (g *Group) Wait() {
	g.WaitForSuccess(func(_ int) bool {
		return false
	})
}

// WaitForNSuccess waits for goroutines ran in parallel
// until enough function returns true. In that case,
// cancel the context for other goroutines to stop
// and wait until other goroutines already started
// to finish execution.
func (g *Group) WaitForSuccess(enough func(_ int) bool) {
	if enough == nil {
		return
	}

	i := 0
	for fnIndex := range g.tasks {
		i++
		if i == g.count {
			break
		}
		if enough(fnIndex) {
			break
		}
	}

	g.cancel()
	g.wg.Wait()
	return
}

// Go calls the given function in a new goroutine.
func (g *Group) Go(f func(context.Context), index int) {
	select {
	case <-g.ctx.Done():
	default:
		g.wg.Add(1)
		go func(i int) {
			f(g.ctx)
			g.tasks <- i
			g.wg.Done()
		}(index)
	}
}
