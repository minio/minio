package log

import (
	internalLog "log"
	"sync"
)

// Global variable to be set
var verbosity = NormalLOG
var mutex sync.RWMutex

func verify(level Level) bool {
	mutex.RLock()
	defer mutex.RUnlock()
	return Level(verbosity) >= level
}

// SetVerbosity - set log level value globally
func SetVerbosity(level Level) {
	mutex.Lock()
	defer mutex.Unlock()
	verbosity = level
}

// Log - variable logging against global verbosity
func Log(level Level, args ...interface{}) {
	if verify(level) {
		internalLog.Print(args...)
	}
}

// Logf - variable formatted logging against global verbosity
func Logf(level Level, s string, args ...interface{}) {
	if verify(level) {
		internalLog.Printf(s, args...)
	}
}

// Logln - variable logging with newline against global verbosity
func Logln(level Level, args ...interface{}) {
	if verify(level) {
		internalLog.Println(args...)
	}
}
