package log

import (
	internalLog "log"
)

// Print - Normal logging
func Print(args ...interface{}) {
	if verify(NormalLOG) {
		internalLog.Print(args...)
	}
}

// Printf - Normal formatted logging
func Printf(s string, args ...interface{}) {
	if verify(NormalLOG) {
		internalLog.Printf(s, args...)
	}
}

// Println - Normal logging with newline
func Println(args ...interface{}) {
	if verify(NormalLOG) {
		internalLog.Println(args...)
	}
}
