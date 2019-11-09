/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
