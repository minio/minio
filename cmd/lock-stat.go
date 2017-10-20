/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

// lockStat - encapsulates total, blocked and granted lock counts.
type lockStat struct {
	total   int64
	blocked int64
	granted int64
}

// lockWaiting - updates lock stat when a lock becomes blocked.
func (ls *lockStat) lockWaiting() {
	ls.blocked++
	ls.total++
}

// lockGranted - updates lock stat when a lock is granted.
func (ls *lockStat) lockGranted() {
	ls.blocked--
	ls.granted++
}

// lockTimedOut - updates lock stat when a lock is timed out.
func (ls *lockStat) lockTimedOut() {
	ls.blocked--
	ls.total--
}

// lockRemoved - updates lock stat when a lock is removed, by Unlock
// or ForceUnlock.
func (ls *lockStat) lockRemoved(granted bool) {
	if granted {
		ls.granted--
		ls.total--

	} else {
		ls.blocked--
		ls.total--
	}
}
