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

import (
	"testing"
	"time"
)

// Test print systemState.
func TestPrintLockState(t *testing.T) {
	nsMutex.Lock("testbucket", "1.txt", "11-11")
	sysLockState, err := getSystemLockState()
	if err != nil {
		t.Fatal(err)
	}
	nsMutex.Unlock("testbucket", "1.txt", "11-11")
	sysLockStateMap := map[string]SystemLockState{}
	sysLockStateMap["bucket"] = sysLockState

	// Print lock state.
	printLockState(sysLockStateMap, 0)

	// Print lock state verbose.
	printLockStateVerbose(sysLockStateMap, 0)

	// Does not print any lock state in normal print mode.
	printLockState(sysLockStateMap, 10*time.Second)

	// Does not print any lock state in debug print mode.
	printLockStateVerbose(sysLockStateMap, 10*time.Second)
}
