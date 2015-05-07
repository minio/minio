package featureflags

import (
	"sync"
)

var features = make(map[string]bool)
var lock = &sync.RWMutex{}

// Get feature will return true if the feature is enabled, otherwise false
func Get(feature string) bool {
	lock.RLock()
	defer lock.RUnlock()
	res := features[feature]
	return res
}

// Enable a feature
func Enable(feature string) {
	lock.Lock()
	defer lock.Unlock()
	features[feature] = true
}

// Disable a feature
func Disable(feature string) {
	lock.Lock()
	defer lock.Unlock()
	features[feature] = false
}
