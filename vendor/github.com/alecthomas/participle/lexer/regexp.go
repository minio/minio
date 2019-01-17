package lexer

import (
	"bytes"
	"io"
	"io/ioutil"
	"regexp"
	"unicode/utf8"
)

var eolBytes = []byte("\n")

type regexpDefinition struct {
	re      *regexp.Regexp
	symbols map[string]rune
}

// Regexp creates a lexer definition from a regular expression.
//
// Each named sub-expression in the regular expression matches a token. Anonymous sub-expressions
// will be matched and discarded.
//
// eg.
//
//     	def, err := Regexp(`(?P<Ident>[a-z]+)|(\s+)|(?P<Number>\d+)`)
func Regexp(pattern string) (Definition, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	symbols := map[string]rune{
		"EOF": EOF,
	}
	for i, sym := range re.SubexpNames()[1:] {
		if sym != "" {
			symbols[sym] = EOF - 1 - rune(i)
		}
	}
	return &regexpDefinition{re: re, symbols: symbols}, nil
}

func (d *regexpDefinition) Lex(r io.Reader) (Lexer, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return &regexpLexer{
		pos: Position{
			Filename: NameOfReader(r),
			Line:     1,
			Column:   1,
		},
		b:     b,
		re:    d.re,
		names: d.re.SubexpNames(),
	}, nil
}

func (d *regexpDefinition) Symbols() map[string]rune {
	return d.symbols
}

type regexpLexer struct {
	pos   Position
	b     []byte
	re    *regexp.Regexp
	names []string
}

func (r *regexpLexer) Next() (Token, error) {
nextToken:
	for len(r.b) != 0 {
		matches := r.re.FindSubmatchIndex(r.b)
		if matches == nil || matches[0] != 0 {
			rn, _ := utf8.DecodeRune(r.b)
			return Token{}, Errorf(r.pos, "invalid token %q", rn)
		}
		match := r.b[:matches[1]]
		token := Token{
			Pos:   r.pos,
			Value: string(match),
		}

		// Update lexer state.
		r.pos.Offset += matches[1]
		lines := bytes.Count(match, eolBytes)
		r.pos.Line += lines
		// Update column.
		if lines == 0 {
			r.pos.Column += utf8.RuneCount(match)
		} else {
			r.pos.Column = utf8.RuneCount(match[bytes.LastIndex(match, eolBytes):])
		}
		// Move slice along.
		r.b = r.b[matches[1]:]

		// Finally, assign token type. If it is not a named group, we continue to the next token.
		for i := 2; i < len(matches); i += 2 {
			if matches[i] != -1 {
				if r.names[i/2] == "" {
					continue nextToken
				}
				token.Type = EOF - rune(i/2)
				break
			}
		}

		return token, nil
	}

	return EOFToken(r.pos), nil
}
