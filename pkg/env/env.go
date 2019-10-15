package env

import (
	"os"
	"strings"
)

// Get retrieves the value of the environment variable named
// by the key. If the variable is present in the environment the
// value (which may be empty) is returned. Otherwise it returns
// the specified default value.
func Get(key, defaultValue string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultValue
}

// Lookup retrieves the value of the environment variable named
// by the key. If the variable is present in the environment the
// value (which may be empty) is returned and the boolean is true.
// Otherwise the returned value will be empty and the boolean will
// be false.
func Lookup(key string) (string, bool) { return os.LookupEnv(key) }

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
