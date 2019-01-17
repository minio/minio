package lexer

import (
	"fmt"
	"io"
)

const (
	// EOF represents an end of file.
	EOF rune = -(iota + 1)
)

// EOFToken creates a new EOF token at the given position.
func EOFToken(pos Position) Token {
	return Token{Type: EOF, Pos: pos}
}

// Definition provides the parser with metadata for a lexer.
type Definition interface {
	// Lex an io.Reader.
	Lex(io.Reader) (Lexer, error)
	// Symbols returns a map of symbolic names to the corresponding pseudo-runes for those symbols.
	// This is the same approach as used by text/scanner. For example, "EOF" might have the rune
	// value of -1, "Ident" might be -2, and so on.
	Symbols() map[string]rune
}

// A Lexer returns tokens from a source.
type Lexer interface {
	// Next consumes and returns the next token.
	Next() (Token, error)
}

// A PeekingLexer returns tokens from a source and allows peeking.
type PeekingLexer interface {
	Lexer
	// Peek at the next token.
	Peek(n int) (Token, error)
}

// SymbolsByRune returns a map of lexer symbol names keyed by rune.
func SymbolsByRune(def Definition) map[rune]string {
	out := map[rune]string{}
	for s, r := range def.Symbols() {
		out[r] = s
	}
	return out
}

// NameOfReader attempts to retrieve the filename of a reader.
func NameOfReader(r interface{}) string {
	if nr, ok := r.(interface{ Name() string }); ok {
		return nr.Name()
	}
	return ""
}

// Must takes the result of a Definition constructor call and returns the definition, but panics if
// it errors
//
// eg.
//
// 		lex = lexer.Must(lexer.Build(`Symbol = "symbol" .`))
func Must(def Definition, err error) Definition {
	if err != nil {
		panic(err)
	}
	return def
}

// ConsumeAll reads all tokens from a Lexer.
func ConsumeAll(lexer Lexer) ([]Token, error) {
	tokens := []Token{}
	for {
		token, err := lexer.Next()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
		if token.Type == EOF {
			return tokens, nil
		}
	}
}

// Position of a token.
type Position struct {
	Filename string
	Offset   int
	Line     int
	Column   int
}

func (p Position) GoString() string {
	return fmt.Sprintf("Position{Filename: %q, Offset: %d, Line: %d, Column: %d}",
		p.Filename, p.Offset, p.Line, p.Column)
}

func (p Position) String() string {
	filename := p.Filename
	if filename == "" {
		filename = "<source>"
	}
	return fmt.Sprintf("%s:%d:%d", filename, p.Line, p.Column)
}

// A Token returned by a Lexer.
type Token struct {
	// Type of token. This is the value keyed by symbol as returned by Definition.Symbols().
	Type  rune
	Value string
	Pos   Position
}

// RuneToken represents a rune as a Token.
func RuneToken(r rune) Token {
	return Token{Type: r, Value: string(r)}
}

// EOF returns true if this Token is an EOF token.
func (t Token) EOF() bool {
	return t.Type == EOF
}

func (t Token) String() string {
	if t.EOF() {
		return "<EOF>"
	}
	return t.Value
}

func (t Token) GoString() string {
	return fmt.Sprintf("Token{%d, %q}", t.Type, t.Value)
}

// MakeSymbolTable builds a lookup table for checking token ID existence.
//
// For each symbolic name in "types", the returned map will contain the corresponding token ID as a key.
func MakeSymbolTable(def Definition, types ...string) (map[rune]bool, error) {
	symbols := def.Symbols()
	table := map[rune]bool{}
	for _, symbol := range types {
		rn, ok := symbols[symbol]
		if !ok {
			return nil, fmt.Errorf("lexer does not support symbol %q", symbol)
		}
		table[rn] = true
	}
	return table, nil
}
