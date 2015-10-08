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

import "github.com/minio/minio/pkg/probe"

// Handle as the name suggests is a handle (self reference) to its
// own task structure. Task has limited privileges over itself. Only the
// task controller (TaskCtl) can manage the task by sending commands to
// the task over channels.
type Handle struct {
	this     taskRef
	cmdCh    <-chan Command // Channel to receive commands from TaskCtl.
	statusCh chan<- status  // Channel to send completion status and error (if any) to TaskCtl.
	closeCh  chan<- taskRef // Channel to notify the TaskCtl about ending this task.
}

// Listen returns a channel to receive commands.
func (t Handle) Listen() <-chan Command {
	return t.cmdCh
}

// StatusDone acknowledges successful completion of a command.
func (t Handle) StatusDone() {
	t.statusCh <- status{code: statusDone, err: nil}
}

// StatusBusy rejects a command with busy status.
func (t Handle) StatusBusy() {
	t.statusCh <- status{code: statusBusy, err: nil}
}

// StatusFail returns failure status.
func (t Handle) StatusFail(err *probe.Error) {
	t.statusCh <- status{code: statusFail, err: err}
}

// Close notifies the TaskCtl about the end of this Task. Owner of the
// task must invoke Close() when it is done performing its job.
func (t Handle) Close() {
	t.closeCh <- t.this
}
