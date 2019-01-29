package lexer

import "fmt"

// Error represents an error while parsing.
type Error struct {
	Message string
	Pos     Position
}

// Errorf creats a new Error at the given position.
func Errorf(pos Position, format string, args ...interface{}) *Error {
	return &Error{
		Message: fmt.Sprintf(format, args...),
		Pos:     pos,
	}
}

// Error complies with the error interface and reports the position of an error.
func (e *Error) Error() string {
	filename := e.Pos.Filename
	if filename == "" {
		filename = "<source>"
	}
	return fmt.Sprintf("%s:%d:%d: %s", filename, e.Pos.Line, e.Pos.Column, e.Message)
}
