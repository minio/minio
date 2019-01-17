package lexer

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/scanner"
	"unicode/utf8"
)

// TextScannerLexer is a lexer that uses the text/scanner module.
var (
	TextScannerLexer Definition = &defaultDefinition{}

	// DefaultDefinition defines properties for the default lexer.
	DefaultDefinition = TextScannerLexer
)

type defaultDefinition struct{}

func (d *defaultDefinition) Lex(r io.Reader) (Lexer, error) {
	return Lex(r), nil
}

func (d *defaultDefinition) Symbols() map[string]rune {
	return map[string]rune{
		"EOF":       scanner.EOF,
		"Char":      scanner.Char,
		"Ident":     scanner.Ident,
		"Int":       scanner.Int,
		"Float":     scanner.Float,
		"String":    scanner.String,
		"RawString": scanner.RawString,
		"Comment":   scanner.Comment,
	}
}

// textScannerLexer is a Lexer based on text/scanner.Scanner
type textScannerLexer struct {
	scanner  *scanner.Scanner
	filename string
	err      error
}

// Lex an io.Reader with text/scanner.Scanner.
//
// This provides very fast lexing of source code compatible with Go tokens.
//
// Note that this differs from text/scanner.Scanner in that string tokens will be unquoted.
func Lex(r io.Reader) Lexer {
	lexer := lexWithScanner(r, &scanner.Scanner{})
	lexer.scanner.Error = func(s *scanner.Scanner, msg string) {
		// This is to support single quoted strings. Hacky.
		if msg != "illegal char literal" {
			lexer.err = Errorf(Position(lexer.scanner.Pos()), msg)
		}
	}
	return lexer
}

// LexWithScanner creates a Lexer from a user-provided scanner.Scanner.
//
// Useful if you need to customise the Scanner.
func LexWithScanner(r io.Reader, scan *scanner.Scanner) Lexer {
	return lexWithScanner(r, scan)
}

func lexWithScanner(r io.Reader, scan *scanner.Scanner) *textScannerLexer {
	lexer := &textScannerLexer{
		filename: NameOfReader(r),
		scanner:  scan,
	}
	lexer.scanner.Init(r)
	return lexer
}

// LexBytes returns a new default lexer over bytes.
func LexBytes(b []byte) Lexer {
	return Lex(bytes.NewReader(b))
}

// LexString returns a new default lexer over a string.
func LexString(s string) Lexer {
	return Lex(strings.NewReader(s))
}

func (t *textScannerLexer) Next() (Token, error) {
	typ := t.scanner.Scan()
	text := t.scanner.TokenText()
	pos := Position(t.scanner.Position)
	pos.Filename = t.filename
	if t.err != nil {
		return Token{}, t.err
	}
	return textScannerTransform(Token{
		Type:  typ,
		Value: text,
		Pos:   pos,
	})
}

func textScannerTransform(token Token) (Token, error) {
	// Unquote strings.
	switch token.Type {
	case scanner.Char:
		// FIXME(alec): This is pretty hacky...we convert a single quoted char into a double
		// quoted string in order to support single quoted strings.
		token.Value = fmt.Sprintf("\"%s\"", token.Value[1:len(token.Value)-1])
		fallthrough
	case scanner.String:
		s, err := strconv.Unquote(token.Value)
		if err != nil {
			return Token{}, Errorf(token.Pos, "%s: %q", err.Error(), token.Value)
		}
		token.Value = s
		if token.Type == scanner.Char && utf8.RuneCountInString(s) > 1 {
			token.Type = scanner.String
		}
	case scanner.RawString:
		token.Value = token.Value[1 : len(token.Value)-1]
	}
	return token, nil
}
