package jstream

import (
	"fmt"
	"strconv"
)

// Predefined errors
var (
	ErrSyntax        = SyntaxError{msg: "invalid character"}
	ErrUnexpectedEOF = SyntaxError{msg: "unexpected end of JSON input"}
)

type errPos [2]int // line number, byte offset where error occurred

type SyntaxError struct {
	msg     string // description of error
	context string // additional error context
	pos     errPos
	atChar  byte
}

func (e SyntaxError) Error() string {
	loc := fmt.Sprintf("%s [%d,%d]", quoteChar(e.atChar), e.pos[0], e.pos[1])
	return fmt.Sprintf("%s %s: %s", e.msg, e.context, loc)
}

// quoteChar formats c as a quoted character literal
func quoteChar(c byte) string {
	// special cases - different from quoted strings
	if c == '\'' {
		return `'\''`
	}
	if c == '"' {
		return `'"'`
	}

	// use quoted string with different quotation marks
	s := strconv.Quote(string(c))
	return "'" + s[1:len(s)-1] + "'"
}
