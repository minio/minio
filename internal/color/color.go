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

package color

import (
	"fmt"

	"github.com/fatih/color"
)

// global colors.
var (
	// Check if we stderr, stdout are dumb terminals, we do not apply
	// ansi coloring on dumb terminals.
	IsTerminal = func() bool {
		return !color.NoColor
	}

	Bold = func() func(a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.Bold).SprintFunc()
		}
		return fmt.Sprint
	}()

	RedBold = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgRed, color.Bold).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	Red = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgRed).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	Blue = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgBlue).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	Yellow = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgYellow).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	Green = func() func(a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgGreen).SprintFunc()
		}
		return fmt.Sprint
	}()

	GreenBold = func() func(a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgGreen, color.Bold).SprintFunc()
		}
		return fmt.Sprint
	}()

	CyanBold = func() func(a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgCyan, color.Bold).SprintFunc()
		}
		return fmt.Sprint
	}()

	YellowBold = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgYellow, color.Bold).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	BlueBold = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgBlue, color.Bold).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	BgYellow = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.BgYellow).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	Black = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgBlack).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	FgRed = func() func(a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgRed).SprintFunc()
		}
		return fmt.Sprint
	}()

	BgRed = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.BgRed).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	FgWhite = func() func(format string, a ...interface{}) string {
		if IsTerminal() {
			return color.New(color.FgWhite).SprintfFunc()
		}
		return fmt.Sprintf
	}()
)
