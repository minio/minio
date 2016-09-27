/*
 * Minio Cloud Storage, (C) 2014-2016 Minio, Inc.
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
	"os"
	"testing"
)

// tests that setGlobalsDebugFromEnv sets globalDebugLock or
// globalDebugMemory to true, along with globalDebug to true,
// according to the value of MINIO_DEBUG environment variable.
func TestGlobalsDebugFromEnv(t *testing.T) {
	// save env var and global var values
	oldDebugEnv := os.Getenv("MINIO_DEBUG")
	oldGlobalDebug := globalDebug
	oldGlobalDebugLock := globalDebugLock
	oldGlobalDebugMemory := globalDebugMemory

	// set env var for testing
	os.Setenv("MINIO_DEBUG", "lock")

	// first test
	setGlobalsDebugFromEnv()
	if !globalDebugLock || !globalDebug {
		t.Errorf("setGlobalsDebugFromEnv failed with MINIO_DEBUG=lock")
	}

	// restore old values
	globalDebug = oldGlobalDebug
	globalDebugLock = oldGlobalDebugLock
	globalDebugMemory = oldGlobalDebugMemory

	// set env var for testing again
	os.Setenv("MINIO_DEBUG", "mem")

	// second test
	setGlobalsDebugFromEnv()
	if !globalDebugMemory || !globalDebug {
		t.Errorf("setGlobalsDebugFromEnv failed with MINIO_DEBUG=mem")
	}

	// restore all old values
	globalDebug = oldGlobalDebug
	globalDebugLock = oldGlobalDebugLock
	globalDebugMemory = oldGlobalDebugMemory
	os.Setenv("MINIO_DEBUG", oldDebugEnv)
}
