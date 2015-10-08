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

// Command is number that uniquely identifies a command function.
type Command uint8

// Enumerate the task commands.
const (
	// CmdNOOP does nothing. It is a default placeholder. Uninitialized variable of this type will point to NOOP command by default.
	CmdNOOP Command = iota
	// CmdSignalEnd gracefully ends current task. Never ending tasks (loop over) or Batched jobs will not take the next iteration,
	// but may finish the current state to completion.
	CmdSignalEnd
	// CmdSignalAbort ends the current task at hand immediately. It may still cleanup dangling issues quickly.
	CmdSignalAbort
	// CmdSignalSuspend suspends the current task.
	CmdSignalSuspend
	// CmdSignalResume resumes a suspended task.
	CmdSignalResume
	// CmdPriorityLow is optimized to conserve resources and complete the task at a slow pace. This option is ideal for batch processed tasks.
	CmdPriorityLow
	// CmdPriorityMedium is the default priority. It is a balanced option between resources and speed.
	CmdPriorityMedium
	// CmdPriorityHigh is optimized for speed. This option is ideal for short lived tasks (like meta-data related) that are latency sensitive. Use this option wisely.
	CmdPriorityHigh
	// CmdPrioritySuper is an exclusive priority. All tasks with priority lower than Super (including High) are paused
	// temporarily until this task completes. Anytime you consider using this priority level, please seek for approval.
	CmdPrioritySuper
)
