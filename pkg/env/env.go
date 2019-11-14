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
