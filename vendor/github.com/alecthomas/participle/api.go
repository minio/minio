package participle

import (
	"github.com/alecthomas/participle/lexer"
)

// Capture can be implemented by fields in order to transform captured tokens into field values.
type Capture interface {
	Capture(values []string) error
}

// The Parseable interface can be implemented by any element in the grammar to provide custom parsing.
type Parseable interface {
	// Parse into the receiver.
	//
	// Should return NextMatch if no tokens matched and parsing should continue.
	// Nil should be returned if parsing was successful.
	Parse(lex lexer.PeekingLexer) error
}
