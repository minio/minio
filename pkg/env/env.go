// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package env

import (
	"strconv"
	"strings"
	"sync"
)

var (
	privateMutex sync.RWMutex
	lockEnvMutex sync.Mutex
	envOff       bool
)

// LockSetEnv locks modifications to environment.
// Call returned function to unlock.
func LockSetEnv() func() {
	lockEnvMutex.Lock()
	return lockEnvMutex.Unlock
}

// SetEnvOff - turns off env lookup
// A global lock above this MUST ensure that
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
	_, _, _, ok := LookupEnv(key)
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
	if v, _, _, ok := LookupEnv(key); ok {
		return v
	}
	return defaultValue
}

// GetInt returns an integer if found in the environment
// and returns the default value otherwise.
func GetInt(key string, defaultValue int) (int, error) {
	v := Get(key, "")
	if v == "" {
		return defaultValue, nil
	}
	return strconv.Atoi(v)
}

// List all envs with a given prefix.
func List(prefix string) (envs []string) {
	for _, env := range Environ() {
		if strings.HasPrefix(env, prefix) {
			values := strings.SplitN(env, "=", 2)
			if len(values) == 2 {
				envs = append(envs, values[0])
			}
		}
	}
	return envs
}
