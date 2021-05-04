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

// Package console implements console printing helpers
package console

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

var (
	// Used by the caller to print multiple lines atomically. Exposed by Lock/Unlock methods.
	publicMutex sync.Mutex

	// Used internally by console.
	privateMutex sync.Mutex

	stderrColoredOutput = colorable.NewColorableStderr()

	// Print prints a message.
	Print = func(data ...interface{}) {
		consolePrint("Print", Theme["Print"], data...)
	}

	// PrintC prints a message with color.
	PrintC = func(data ...interface{}) {
		consolePrint("PrintC", Theme["PrintC"], data...)
	}

	// Printf prints a formatted message.
	Printf = func(format string, data ...interface{}) {
		consolePrintf("Print", Theme["Print"], format, data...)
	}

	// Println prints a message with a newline.
	Println = func(data ...interface{}) {
		consolePrintln("Print", Theme["Print"], data...)
	}

	// Fatal print a error message and exit.
	Fatal = func(data ...interface{}) {
		consolePrint("Fatal", Theme["Fatal"], data...)
		os.Exit(1)
	}

	// Fatalf print a error message with a format specified and exit.
	Fatalf = func(format string, data ...interface{}) {
		consolePrintf("Fatal", Theme["Fatal"], format, data...)
		os.Exit(1)
	}

	// Fatalln print a error message with a new line and exit.
	Fatalln = func(data ...interface{}) {
		consolePrintln("Fatal", Theme["Fatal"], data...)
		os.Exit(1)
	}

	// Error prints a error message.
	Error = func(data ...interface{}) {
		consolePrint("Error", Theme["Error"], data...)
	}

	// Errorf print a error message with a format specified.
	Errorf = func(format string, data ...interface{}) {
		consolePrintf("Error", Theme["Error"], format, data...)
	}

	// Errorln prints a error message with a new line.
	Errorln = func(data ...interface{}) {
		consolePrintln("Error", Theme["Error"], data...)
	}

	// Info prints a informational message.
	Info = func(data ...interface{}) {
		consolePrint("Info", Theme["Info"], data...)
	}

	// Infof prints a informational message in custom format.
	Infof = func(format string, data ...interface{}) {
		consolePrintf("Info", Theme["Info"], format, data...)
	}

	// Infoln prints a informational message with a new line.
	Infoln = func(data ...interface{}) {
		consolePrintln("Info", Theme["Info"], data...)
	}

	// Debug prints a debug message without a new line
	// Debug prints a debug message.
	Debug = func(data ...interface{}) {
		consolePrint("Debug", Theme["Debug"], data...)
	}

	// Debugf prints a debug message with a new line.
	Debugf = func(format string, data ...interface{}) {
		consolePrintf("Debug", Theme["Debug"], format, data...)
	}

	// Debugln prints a debug message with a new line.
	Debugln = func(data ...interface{}) {
		consolePrintln("Debug", Theme["Debug"], data...)
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

// Table - data to print in table format with fixed row widths.
type Table struct {
	// per-row colors
	RowColors []*color.Color

	// per-column align-right flag (aligns left by default)
	AlignRight []bool

	// Left margin width for table
	TableIndentWidth int

	// Flag to print separator under heading. Row 0 is considered heading
	HeaderRowSeparator bool
}

// NewTable - create a new Table instance. Takes per-row colors and
// per-column right-align flags and table indentation width (i.e. left
// margin width)
func NewTable(rowColors []*color.Color, alignRight []bool, indentWidth int) *Table {
	return &Table{rowColors, alignRight, indentWidth, false}
}

// DisplayTable - prints the table
func (t *Table) DisplayTable(rows [][]string) error {
	numRows := len(rows)
	numCols := len(rows[0])
	if numRows != len(t.RowColors) {
		return fmt.Errorf("row count and row-colors mismatch")
	}

	// Compute max. column widths
	maxColWidths := make([]int, numCols)
	for _, row := range rows {
		if len(row) != len(t.AlignRight) {
			return fmt.Errorf("col count and align-right mismatch")
		}
		for i, v := range row {
			if len([]rune(v)) > maxColWidths[i] {
				maxColWidths[i] = len([]rune(v))
			}
		}
	}

	// Compute per-cell text with padding and alignment applied.
	paddedText := make([][]string, numRows)
	for r, row := range rows {
		paddedText[r] = make([]string, numCols)
		for c, cell := range row {
			if t.AlignRight[c] {
				fmtStr := fmt.Sprintf("%%%ds", maxColWidths[c])
				paddedText[r][c] = fmt.Sprintf(fmtStr, cell)
			} else {
				extraWidth := maxColWidths[c] - len([]rune(cell))
				fmtStr := fmt.Sprintf("%%s%%%ds", extraWidth)
				paddedText[r][c] = fmt.Sprintf(fmtStr, cell, "")
			}
		}
	}

	// Draw table top border
	segments := make([]string, numCols)
	for i, c := range maxColWidths {
		segments[i] = strings.Repeat("─", c+2)
	}
	indentText := strings.Repeat(" ", t.TableIndentWidth)
	border := fmt.Sprintf("%s┌%s┐", indentText, strings.Join(segments, "┬"))
	fmt.Println(border)

	// Print the table with colors
	for r, row := range paddedText {
		if t.HeaderRowSeparator && r == 1 {
			// Draw table header-row border
			border = fmt.Sprintf("%s├%s┤", indentText, strings.Join(segments, "┼"))
			fmt.Println(border)
		}
		fmt.Print(indentText + "│ ")
		for c, text := range row {
			t.RowColors[r].Print(text)
			if c != numCols-1 {
				fmt.Print(" │ ")
			}
		}
		fmt.Println(" │")
	}

	// Draw table bottom border
	border = fmt.Sprintf("%s└%s┘", indentText, strings.Join(segments, "┴"))
	fmt.Println(border)

	return nil
}

// RewindLines - uses terminal escape symbols to clear and rewind
// upwards on the console for `n` lines.
func RewindLines(n int) {
	for i := 0; i < n; i++ {
		fmt.Printf("\033[1A\033[K")
	}
}
