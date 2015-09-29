// Copyright 2014 Oleku Konko All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

// This module is a Terminal  API for the Go Programming Language.
// The protocols were written in pure Go and works on windows and unix systems

package ts

import (
	"fmt"
	"testing"
)

func ExampleGetSize() {
	size, _ := GetSize()
	fmt.Println(size.Col())  // Get Width
	fmt.Println(size.Row())  // Get Height
	fmt.Println(size.PosX()) // Get X position
	fmt.Println(size.PosY()) // Get Y position
}

func TestSize(t *testing.T) {
	size, err := GetSize()

	if err != nil {
		t.Fatal(err)
	}
	if size.Col() == 0 || size.Row() == 0 {
		t.Fatalf("Screen Size Failed")
	}
}
