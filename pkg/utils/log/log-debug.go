package log

import (
	internalLog "log"
)

// Debug logging
func Debug(args ...interface{}) {
	if verify(DebugLOG) {
		internalLog.Print(args...)
	}
}

// Debugf formatted debug logging
func Debugf(s string, args ...interface{}) {
	if verify(DebugLOG) {
		internalLog.Printf(s, args...)
	}
}

// Debugln logging with newline
func Debugln(args ...interface{}) {
	if verify(DebugLOG) {
		internalLog.Println(args...)
	}
}
