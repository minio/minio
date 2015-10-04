// Copyright 2014 shiena Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// +build windows

package ansicolor

import "syscall"

var GetConsoleScreenBufferInfo = getConsoleScreenBufferInfo

func ChangeColor(color uint16) {
	setConsoleTextAttribute(uintptr(syscall.Stdout), color)
}

func ResetColor() {
	ChangeColor(uint16(0x0007))
}
