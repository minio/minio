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

// NOTE: Task is a private entity. It is created and managed by TaskCtl
//      entirely. Only TaskCtl and Handle objects are exposed outside.

// taskRef is a unique reference ID to a task. It is assigned by the
// TaskCtl during the creation of a task. All tasfRef variables are
// named "this".
type taskRef *list.Element

// Task is an abstract concept built on top of Go routines and
// channels. Tasks themselves are expected to co-operate and comply with
// the TaskCtl commands.

type task struct {
	mutex *sync.Mutex

	this     taskRef      // Refence to task entry in the TaskCtl's task list.
	name     string       // Free form name.
	priority Command      // Current priority.
	cmdCh    chan Command // Channel to receive commands from TaskCtl.
	statusCh chan status  // Channel to send completion status and error (if any) to TaskCtl.
	closeCh  chan taskRef // Channel to notify the TaskCtl about ending this task.
}

// NewTask creates a new task structure and returns a handle to
// it. Only the task controller has access to the task structure. The
// caller routine only receives a handle to its task structure. Task
// handle is like a reference to task self. Caller is expected to listen
// for commands from the task controller and comply with it co-operatively.
// this: Task reference is unique identifier assigned by the TaskCtl.
// name: Free form name of the task. Eg. "Late Night Disk Scrubber".
func newTask(name string) task {
	return task{
		// this: Is set by the TaskCtl's NewTask function.
		mutex:    &sync.Mutex{},
		name:     name,
		priority: CmdPriorityMedium,
		cmdCh:    make(chan Command),
		statusCh: make(chan status),
		closeCh:  make(chan taskRef),
	}
}

// getHandle returns a handle to the task. Handle has limited access to the task structure and it is safe to be exposed.
func (t task) getHandle() Handle {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Make a handle with limited access to channels (only send or receive).
	return Handle{
		cmdCh:    t.cmdCh,
		statusCh: t.statusCh,
		closeCh:  t.closeCh,
	}
}

// command method sends a command code to the task and returns its completion status.
func (t task) command(cmd Command) status {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.cmdCh <- cmd
	return <-t.statusCh
}

// close releases all the resources held by this task.
func (t task) close() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Task can be ended in 2 ways.
	// 1) Calling application invokes Handle.Close().
	// 2) TaskCtl.Shutdown() ending the task's life.
	// In either case, task.close() is invoked only via the
	// TaskCtl. Handle.Close() only sends a message to the TaskCtl to
	// initiate a close call.

	close(t.cmdCh)
	close(t.statusCh)
	close(t.closeCh)
}
