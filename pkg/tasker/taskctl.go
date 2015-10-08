/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package tasker

import (
	"container/list"
	"sync"
)

// TaskCtl (Task Controller) is a framework to create and manage
// tasks.
type TaskCtl struct {
	mutex *sync.Mutex // Lock
	// List of tasks managed by this task controller.
	tasks *list.List
}

// New creates a new TaskCtl to create and control a collection of tasks.
// Single application can create multiple task controllers to manage different set of tasks separately.
func New(name string) *TaskCtl {
	return &TaskCtl{
		mutex: &sync.Mutex{},
		tasks: list.New(),
	}
}

// NewTask creates a new task structure and returns a handle to it. Only the task controller
// has access to the task structure. The caller routine only receives a handle to its task structure.
// Task handle is like a reference to task self. Caller is expected to listen for commands from
// the task controller and comply with it co-operatively.
func (tc *TaskCtl) NewTask(name string) Handle {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	// Create a new task.
	tsk := newTask(name)

	// Register this task in the TaskCtl's tasklist and save the reference.
	tsk.this = tc.tasks.PushBack(tsk)

	// Free task from the tasklist upon close call.
	go func() {
		// Release the tasks resources upon return of this function.
		defer tsk.close()

		// Will be notified here upon task's end of life.
		this := <-tsk.closeCh

		tc.mutex.Lock()
		defer tc.mutex.Unlock()

		// Release the task structure from the task list.
		tc.tasks.Remove(this)
	}()

	// Return a handle to this task.
	return tsk.getHandle()
}

// Shutdown ends all tasks, including the suspended ones.
func (tc *TaskCtl) Shutdown() {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	var wg sync.WaitGroup

	// End all tasks.
	for e := tc.tasks.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		thisTask := e.Value.(task) // Make a local copy for go routine.
		// End tasks in background. Flow of events from here is as follows: thisTask.handle.Close() -> tc.NewTask() -> this.task.close().
		go func() {
			thisTask.getHandle().Close()
			wg.Done()
		}()
	}

	wg.Wait() // Wait for all tasks to end gracefully.

	// Reset the task pool.
	tc.tasks = nil
}

// Suspend puts all tasks to sleep.
func (tc *TaskCtl) Suspend() bool {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	var wg sync.WaitGroup

	// If any one of the task fails to suspend, this flag will set to false.
	statusAll := make([]status, tc.tasks.Len())

	// Suspend all tasks.
	i := 0
	for e := tc.tasks.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		locTask := e.Value.(task) // Make a local copy for go routine.
		locI := i                 // local i
		// Suspend a task in background.
		go func(locI int) {
			defer wg.Done()
			statusAll[locI] = locTask.command(CmdSignalSuspend)
		}(locI)
		i++
	}

	wg.Wait() // Wait for all tasks to suspend gracefully.

	for _, st := range statusAll {
		if st.code != statusDone {
			return false
		}
	}
	return true
}

// Resume wakes up all suspended task from sleep.
func (tc *TaskCtl) Resume() bool {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	var wg sync.WaitGroup

	// If any one of the task fails to suspend, this flag will set to false.
	statusAll := make([]status, tc.tasks.Len())

	i := 0
	// Resume all suspended tasks.
	for e := tc.tasks.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		locTask := e.Value.(task) // Make a local copy for go routine.
		locI := i                 // local i
		// Resume a task in background.
		go func(locI int) {
			defer wg.Done()
			statusAll[locI] = locTask.command(CmdSignalResume)
		}(locI)
		i++
	}
	wg.Wait() // Wait for all tasks to resume.

	for _, st := range statusAll {
		if st.code != statusDone {
			return false
		}
	}
	return true

}
