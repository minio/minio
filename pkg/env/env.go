/*
 * MinIO Cloud Storage, (C) 2019-2020 MinIO, Inc.
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
 *
 */

package env

import (
	"os"
	"strings"
	"sync"
)

var (
	privateMutex sync.RWMutex
	envOff       bool
)

// SetEnvOff - turns off env lookup
func SetEnvOff() {
	privateMutex.Lock()
	defer privateMutex.Unlock()

	envOff = true
}

// SetEnvOn - turns on env lookup
func SetEnvOn() {
	privateMutex.Lock()
	defer privateMutex.Unlock()

	envOff = false
}

// IsSet returns if the given env key is set.
func IsSet(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

// Get retrieves the value of the environment variable named
// by the key. If the variable is present in the environment the
// value (which may be empty) is returned. Otherwise it returns
// the specified default value.
func Get(key, defaultValue string) string {
	privateMutex.RLock()
	ok := envOff
	privateMutex.RUnlock()
	if ok {
		return defaultValue
	}
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultValue
}

// List all envs with a given prefix.
func List(prefix string) (envs []string) {
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, prefix) {
			values := strings.SplitN(env, "=", 2)
			if len(values) == 2 {
				envs = append(envs, values[0])
			}
		}
	}
	return envs
}
