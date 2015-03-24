package log

import (
	internalLog "log"
)

// Fatal - prints string and terminates
var Fatal = internalLog.Fatal

// Fatalf - prints formatted string and terminates
var Fatalf = internalLog.Fatalf

// Fatalln - prints with newline and terminates
var Fatalln = internalLog.Fatalln

// Error - prints string but does not terminate
func Error(args ...interface{}) { internalLog.Print(args...) }

// Errorf - prints formatted string but does not terminate
func Errorf(s string, args ...interface{}) { internalLog.Printf(s, args...) }

// Errorln - prints string with newline but does not terminate
func Errorln(args ...interface{}) { internalLog.Println(args...) }

// Level - logging level
type Level int

// Different log levels
const (
	QuietLOG Level = iota
	NormalLOG
	DebugLOG
	TraceLOG
)
