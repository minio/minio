/*
 * Minio Client (C) 2015 Minio, Inc.
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

package console

import (
	"fmt"
	"os"
	"sync"

	"path/filepath"

	"github.com/fatih/color"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

var (
	// DebugPrint enables/disables console debug printing.
	DebugPrint = false

	// Used by the caller to print multiple lines atomically. Exposed by Lock/Unlock methods.
	publicMutex = &sync.Mutex{}
	// Used internally by console.
	privateMutex = &sync.Mutex{}

	stderrColoredOutput = colorable.NewColorableStderr()

	// Print prints a message.
	Print = func(data ...interface{}) {
		consolePrint("Print", Theme["Print"], data...)
		return
	}

	// PrintC prints a message with color.
	PrintC = func(data ...interface{}) {
		consolePrint("PrintC", Theme["PrintC"], data...)
		return
	}

	// Printf prints a formatted message.
	Printf = func(format string, data ...interface{}) {
		consolePrintf("Print", Theme["Print"], format, data...)
		return
	}

	// Println prints a message with a newline.
	Println = func(data ...interface{}) {
		consolePrintln("Print", Theme["Print"], data...)
		return
	}

	// Fatal print a error message and exit.
	Fatal = func(data ...interface{}) {
		consolePrint("Fatal", Theme["Fatal"], data...)
		os.Exit(1)
		return
	}

	// Fatalf print a error message with a format specified and exit.
	Fatalf = func(format string, data ...interface{}) {
		consolePrintf("Fatal", Theme["Fatal"], format, data...)
		os.Exit(1)
		return
	}

	// Fatalln print a error message with a new line and exit.
	Fatalln = func(data ...interface{}) {
		consolePrintln("Fatal", Theme["Fatal"], data...)
		os.Exit(1)
		return
	}

	// Error prints a error message.
	Error = func(data ...interface{}) {
		consolePrint("Error", Theme["Error"], data...)
		return
	}

	// Errorf print a error message with a format specified.
	Errorf = func(format string, data ...interface{}) {
		consolePrintf("Error", Theme["Error"], format, data...)
		return
	}

	// Errorln prints a error message with a new line.
	Errorln = func(data ...interface{}) {
		consolePrintln("Error", Theme["Error"], data...)
		return
	}

	// Info prints a informational message.
	Info = func(data ...interface{}) {
		consolePrint("Info", Theme["Info"], data...)
		return
	}

	// Infof prints a informational message in custom format.
	Infof = func(format string, data ...interface{}) {
		consolePrintf("Info", Theme["Info"], format, data...)
		return
	}

	// Infoln prints a informational message with a new line.
	Infoln = func(data ...interface{}) {
		consolePrintln("Info", Theme["Info"], data...)
		return
	}

	// Debug prints a debug message without a new line
	// Debug prints a debug message.
	Debug = func(data ...interface{}) {
		if DebugPrint {
			consolePrint("Debug", Theme["Debug"], data...)
		}
	}

	// Debugf prints a debug message with a new line.
	Debugf = func(format string, data ...interface{}) {
		if DebugPrint {
			consolePrintf("Debug", Theme["Debug"], format, data...)
		}
	}

	// Debugln prints a debug message with a new line.
	Debugln = func(data ...interface{}) {
		if DebugPrint {
			consolePrintln("Debug", Theme["Debug"], data...)
		}
	}

	// Colorize prints message in a colorized form, dictated by the corresponding tag argument.
	Colorize = func(tag string, data interface{}) string {
		if isatty.IsTerminal(os.Stdout.Fd()) {
			colorized, ok := Theme[tag]
			if ok {
				return colorized.SprintFunc()(data)
			} // else: No theme found. Return as string.
		}
		return fmt.Sprint(data)
	}

	// Eraseline Print in new line and adjust to top so that we don't print over the ongoing progress bar.
	Eraseline = func() {
		consolePrintf("Print", Theme["Print"], "%c[2K\n", 27)
		consolePrintf("Print", Theme["Print"], "%c[A", 27)
	}
)

// wrap around standard fmt functions.
// consolePrint prints a message prefixed with message type and program name.
func consolePrint(tag string, c *color.Color, a ...interface{}) {
	privateMutex.Lock()
	defer privateMutex.Unlock()

	switch tag {
	case "Debug":
		// if no arguments are given do not invoke debug printer.
		if len(a) == 0 {
			return
		}
		output := color.Output
		color.Output = stderrColoredOutput
		if isatty.IsTerminal(os.Stderr.Fd()) {
			c.Print(ProgramName() + ": <DEBUG> ")
			c.Print(a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": <DEBUG> ")
			fmt.Fprint(color.Output, a...)
		}
		color.Output = output
	case "Fatal":
		fallthrough
	case "Error":
		// if no arguments are given do not invoke fatal and error printer.
		if len(a) == 0 {
			return
		}
		output := color.Output
		color.Output = stderrColoredOutput
		if isatty.IsTerminal(os.Stderr.Fd()) {
			c.Print(ProgramName() + ": <ERROR> ")
			c.Print(a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": <ERROR> ")
			fmt.Fprint(color.Output, a...)
		}
		color.Output = output
	case "Info":
		// if no arguments are given do not invoke info printer.
		if len(a) == 0 {
			return
		}
		if isatty.IsTerminal(os.Stdout.Fd()) {
			c.Print(ProgramName() + ": ")
			c.Print(a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": ")
			fmt.Fprint(color.Output, a...)
		}
	default:
		if isatty.IsTerminal(os.Stdout.Fd()) {
			c.Print(a...)
		} else {
			fmt.Fprint(color.Output, a...)
		}
	}
}

// consolePrintf - same as print with a new line.
func consolePrintf(tag string, c *color.Color, format string, a ...interface{}) {
	privateMutex.Lock()
	defer privateMutex.Unlock()

	switch tag {
	case "Debug":
		// if no arguments are given do not invoke debug printer.
		if len(a) == 0 {
			return
		}
		output := color.Output
		color.Output = stderrColoredOutput
		if isatty.IsTerminal(os.Stderr.Fd()) {
			c.Print(ProgramName() + ": <DEBUG> ")
			c.Printf(format, a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": <DEBUG> ")
			fmt.Fprintf(color.Output, format, a...)
		}
		color.Output = output
	case "Fatal":
		fallthrough
	case "Error":
		// if no arguments are given do not invoke fatal and error printer.
		if len(a) == 0 {
			return
		}
		output := color.Output
		color.Output = stderrColoredOutput
		if isatty.IsTerminal(os.Stderr.Fd()) {
			c.Print(ProgramName() + ": <ERROR> ")
			c.Printf(format, a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": <ERROR> ")
			fmt.Fprintf(color.Output, format, a...)
		}
		color.Output = output
	case "Info":
		// if no arguments are given do not invoke info printer.
		if len(a) == 0 {
			return
		}
		if isatty.IsTerminal(os.Stdout.Fd()) {
			c.Print(ProgramName() + ": ")
			c.Printf(format, a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": ")
			fmt.Fprintf(color.Output, format, a...)
		}
	default:
		if isatty.IsTerminal(os.Stdout.Fd()) {
			c.Printf(format, a...)
		} else {
			fmt.Fprintf(color.Output, format, a...)
		}
	}
}

// consolePrintln - same as print with a new line.
func consolePrintln(tag string, c *color.Color, a ...interface{}) {
	privateMutex.Lock()
	defer privateMutex.Unlock()

	switch tag {
	case "Debug":
		// if no arguments are given do not invoke debug printer.
		if len(a) == 0 {
			return
		}
		output := color.Output
		color.Output = stderrColoredOutput
		if isatty.IsTerminal(os.Stderr.Fd()) {
			c.Print(ProgramName() + ": <DEBUG> ")
			c.Println(a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": <DEBUG> ")
			fmt.Fprintln(color.Output, a...)
		}
		color.Output = output
	case "Fatal":
		fallthrough
	case "Error":
		// if no arguments are given do not invoke fatal and error printer.
		if len(a) == 0 {
			return
		}
		output := color.Output
		color.Output = stderrColoredOutput
		if isatty.IsTerminal(os.Stderr.Fd()) {
			c.Print(ProgramName() + ": <ERROR> ")
			c.Println(a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": <ERROR> ")
			fmt.Fprintln(color.Output, a...)
		}
		color.Output = output
	case "Info":
		// if no arguments are given do not invoke info printer.
		if len(a) == 0 {
			return
		}
		if isatty.IsTerminal(os.Stdout.Fd()) {
			c.Print(ProgramName() + ": ")
			c.Println(a...)
		} else {
			fmt.Fprint(color.Output, ProgramName()+": ")
			fmt.Fprintln(color.Output, a...)
		}
	default:
		if isatty.IsTerminal(os.Stdout.Fd()) {
			c.Println(a...)
		} else {
			fmt.Fprintln(color.Output, a...)
		}
	}
}

// Lock console.
func Lock() {
	publicMutex.Lock()
}

// Unlock locked console.
func Unlock() {
	publicMutex.Unlock()
}

// ProgramName - return the name of the executable program.
func ProgramName() string {
	_, progName := filepath.Split(os.Args[0])
	return progName
}
