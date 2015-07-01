/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package pq

import (
	"container/heap"
	"fmt"
	"testing"

	. "github.com/minio/check"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func helloTask1() <-chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		println("Hello task1")
		errCh <- nil
	}()
	return errCh
}

func helloTask2() <-chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		println("Hello task2")
		errCh <- nil
	}()
	return errCh
}

func newJob1() <-chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		println("New Job1")
		errCh <- nil
	}()
	return errCh
}

func newJob2() <-chan error {
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		println("New Job2")
		errCh <- nil
	}()
	return errCh
}

func (s *MySuite) TestPQ(c *C) {
	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := make(PriorityQueue, 2)
	pq[0] = &Item{
		task:  Task{job: helloTask1, priority: 2},
		index: 0,
	}
	pq[1] = &Item{
		task:  Task{job: helloTask2, priority: 1},
		index: 1,
	}
	heap.Init(&pq)

	// Insert a new item and then modify its priority.
	item := &Item{
		task: Task{job: newJob1, priority: 5},
	}
	heap.Push(&pq, item)
	newTask := Task{job: newJob2, priority: 6}
	pq.Fix(item, newTask)

	// Take the items out; they arrive in decreasing priority order.
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		fmt.Printf("%.2d", item.task.GetPriority())
		item.task.Execute()
	}
}
