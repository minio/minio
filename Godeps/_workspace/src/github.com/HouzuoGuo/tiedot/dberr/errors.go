package dberr

import "fmt"

type errorType string

const (
	ErrorNil       errorType = ""
	ErrorUndefined errorType = "Unknown Error."

	// IO error
	ErrorIO    errorType = "IO error has occured, see log for more details."
	ErrorNoDoc errorType = "Document `%d` does not exist"

	// Document errors
	ErrorDocTooLarge errorType = "Document is too large. Max: `%d`, Given: `%d`"
	ErrorDocLocked   errorType = "Document `%d` is locked for update - try again later"

	// Query input errors
	ErrorNeedIndex         errorType = "Please index %v and retry query %v."
	ErrorExpectingSubQuery errorType = "Expecting a vector of sub-queries, but %v given."
	ErrorExpectingInt      errorType = "Expecting `%s` as an integer, but %v given."
	ErrorMissing           errorType = "Missing `%s`"
)

func New(err errorType, details ...interface{}) Error {
	return Error{err, details}
}

type Error struct {
	err     errorType
	details []interface{}
}

func (e Error) Error() string {
	return fmt.Sprintf(string(e.err), e.details...)
}

func Type(e error) errorType {
	if e == nil {
		return ErrorNil
	}

	if err, ok := e.(Error); ok {
		return err.err
	}
	return ErrorUndefined
}
