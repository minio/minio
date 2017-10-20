/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package lsync

import (
	"sync"
	"sync/atomic"
)

// LFrequentAccess is a synchronization mechanism for frequently read yet
// infrequently updated data structures. It uses a copy-on-write paradigm
// for updates to the data.
type LFrequentAccess struct {
	state     atomic.Value
	writeLock sync.Mutex
	locked    bool
}

// NewLFrequentAccess - initializes a new LFrequentAccess.
func NewLFrequentAccess(x interface{}) *LFrequentAccess {
	lm := &LFrequentAccess{}
	lm.state.Store(x)
	return lm
}

// ReadOnlyAccess returns the data intented for reads without further synchronization
func (lm *LFrequentAccess) ReadOnlyAccess() (constReadOnly interface{}) {
	return lm.state.Load()
}

// LockBeforeSet must be called before updates of the data in order to synchronize
// with other potential writers. It returns the current version of the data that
// needs to be copied over into a new version.
func (lm *LFrequentAccess) LockBeforeSet() (constCurVersion interface{}) {
	lm.writeLock.Lock()
	lm.locked = true
	return lm.state.Load()
}

// SetNewCopyAndUnlock updates the data with a new modified copy and unlocks
// simultaneously. Make sure to call LockBeforeSet beforehand to synchronize
// between potential parallel writers (and not lose any updated information).
func (lm *LFrequentAccess) SetNewCopyAndUnlock(newCopy interface{}) {
	if !lm.locked {
		panic("SetNewCopyAndUnlock: locked state is false (did you call LockBeforeSet?)")
	}
	lm.state.Store(newCopy)
	lm.locked = false
	lm.writeLock.Unlock()
}
