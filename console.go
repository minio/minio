/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package main

import (
	"fmt"
	"os"
	"sync"

	"path/filepath"
)

type logLevel int

const (
	levelUnknown logLevel = iota
	levelPrint
	levelDebug
	levelInfo
	levelError
	levelFatal
)

var (
	mutex = &sync.RWMutex{}

	// Fatal prints a error message and exits
	Fatal = func(a ...interface{}) { privatePrint(levelFatal, a...) }
	// Fatalln prints a error message with a new line and exits
	Fatalln = func(a ...interface{}) { privatePrintln(levelFatal, a...) }
	// Fatalf prints a error message with formatting and exits
	Fatalf = func(f string, a ...interface{}) { privatePrintf(levelFatal, f, a...) }

	// Error prints a error message
	Error = func(a ...interface{}) { privatePrint(levelError, a...) }
	// Errorln prints a error message with a new line
	Errorln = func(a ...interface{}) { privatePrintln(levelError, a...) }
	// Errorf prints a error message with formatting
	Errorf = func(f string, a ...interface{}) { privatePrintf(levelError, f, a...) }

	// Info prints a informational message
	Info = func(a ...interface{}) { privatePrint(levelInfo, a...) }
	// Infoln prints a informational message with a new line
	Infoln = func(a ...interface{}) { privatePrintln(levelInfo, a...) }
	// Infof prints a informational message with formatting
	Infof = func(f string, a ...interface{}) { privatePrintf(levelInfo, f, a...) }

	// Debug prints a debug message
	Debug = func(a ...interface{}) { privatePrint(levelDebug, a...) }
	// Debugln prints a debug message with a new line
	Debugln = func(a ...interface{}) { privatePrintln(levelDebug, a...) }
	// Debugf prints a debug message with formatting
	Debugf = func(f string, a ...interface{}) { privatePrintf(levelDebug, f, a...) }

	// Print prints a debug message
	Print = func(a ...interface{}) { privatePrint(levelPrint, a...) }
	// Println prints a debug message with a new line
	Println = func(a ...interface{}) { privatePrintln(levelPrint, a...) }
	// Printf prints a debug message with formatting
	Printf = func(f string, a ...interface{}) { privatePrintf(levelPrint, f, a...) }
)

var (
	// print prints a message prefixed with message type and program name
	privatePrint = func(l logLevel, a ...interface{}) {
		switch l {
		case levelDebug:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <DEBUG> ")
			fmt.Fprint(os.Stderr, a...)
			mutex.Unlock()
		case levelInfo:
			mutex.Lock()
			fmt.Print(ProgramName() + ": <INFO> ")
			fmt.Print(a...)
			mutex.Unlock()
		case levelError:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <ERROR> ")
			fmt.Fprint(os.Stderr, a...)
			mutex.Unlock()
		case levelFatal:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <FATAL> ")
			fmt.Fprint(os.Stderr, a...)
			mutex.Unlock()
			os.Exit(1)
		default:
			fmt.Print(a...)
		}
	}

	// println - same as print with a new line
	privatePrintln = func(l logLevel, a ...interface{}) {
		switch l {
		case levelDebug:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <DEBUG> ")
			fmt.Fprintln(os.Stderr, a...)
			mutex.Unlock()
		case levelInfo:
			mutex.Lock()
			fmt.Print(ProgramName() + ": <INFO> ")
			fmt.Println(a...)
			mutex.Unlock()
		case levelError:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <ERROR> ")
			fmt.Fprintln(os.Stderr, a...)
			mutex.Unlock()
		case levelFatal:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <FATAL> ")
			fmt.Fprintln(os.Stderr, a...)
			mutex.Unlock()
			os.Exit(1)
		default:
			fmt.Println(a...)
		}
	}

	// printf - same as print, but takes a format specifier
	privatePrintf = func(l logLevel, f string, a ...interface{}) {
		switch l {
		case levelDebug:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <DEBUG> ")
			fmt.Fprintf(os.Stderr, f, a...)
			mutex.Unlock()
		case levelInfo:
			mutex.Lock()
			fmt.Print(ProgramName() + ": <INFO> ")
			fmt.Printf(f, a...)
			mutex.Unlock()
		case levelError:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <ERROR> ")
			fmt.Fprintf(os.Stderr, f, a...)
			mutex.Unlock()

		case levelFatal:
			mutex.Lock()
			fmt.Fprint(os.Stderr, ProgramName()+": <FATAL> ")
			fmt.Fprintf(os.Stderr, f, a...)
			mutex.Unlock()
			os.Exit(1)
		default:
			fmt.Printf(f, a...)
		}
	}
)

// ProgramName - return the name of the executable program
func ProgramName() string {
	_, progName := filepath.Split(os.Args[0])
	return progName
}
