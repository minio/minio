// Copyright 2014 shiena Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package ansicolor provides color console in Windows as ANSICON.
package ansicolor

import "io"

// NewAnsiColorWriter creates and initializes a new ansiColorWriter
// using io.Writer w as its initial contents.
// In the console of Windows, which change the foreground and background
// colors of the text by the escape sequence.
// In the console of other systems, which writes to w all text.
func NewAnsiColorWriter(w io.Writer) io.Writer {
	if _, ok := w.(*ansiColorWriter); !ok {
		return &ansiColorWriter{w: w}
	}
	return w
}
