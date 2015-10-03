// Copyright 2014 Oleku Konko All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

// This module is a Terminal  API for the Go Programming Language.
// The protocols were written in pure Go and works on windows and unix systems

/**

Simple go Application to get Terminal Size. So Many Implementations do not support windows but `ts` has full windows support.
Run `go get github.com/olekukonko/ts` to download and install

Installation

Minimum requirements are Go 1.1+ with fill Windows support

Example

	package main

	import (
		"fmt"
		"github.com/olekukonko/ts"
	)

	func main() {
		size, _ := ts.GetSize()
		fmt.Println(size.Col())  // Get Width
		fmt.Println(size.Row())  // Get Height
		fmt.Println(size.PosX()) // Get X position
		fmt.Println(size.PosY()) // Get Y position
	}

**/

package ts
