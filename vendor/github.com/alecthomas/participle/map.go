package participle

import (
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/alecthomas/participle/lexer"
)

type mapperByToken struct {
	symbols []string
	mapper  Mapper
}

// DropToken can be returned by a Mapper to remove a token from the stream.
var DropToken = errors.New("drop token") // nolint: golint

// Mapper function for mutating tokens before being applied to the AST.
//
// If the Mapper func returns an error of DropToken, the token will be removed from the stream.
type Mapper func(token lexer.Token) (lexer.Token, error)

// Map is an Option that configures the Parser to apply a mapping function to each Token from the lexer.
//
// This can be useful to eg. upper-case all tokens of a certain type, or dequote strings.
//
// "symbols" specifies the token symbols that the Mapper will be applied to. If empty, all tokens will be mapped.
func Map(mapper Mapper, symbols ...string) Option {
	return func(p *Parser) error {
		p.mappers = append(p.mappers, mapperByToken{
			mapper:  mapper,
			symbols: symbols,
		})
		return nil
	}
}

// Unquote applies strconv.Unquote() to tokens of the given types.
//
// Tokens of type "String" will be unquoted if no other types are provided.
func Unquote(types ...string) Option {
	if len(types) == 0 {
		types = []string{"String"}
	}
	return Map(func(t lexer.Token) (lexer.Token, error) {
		value, err := unquote(t.Value)
		if err != nil {
			return t, lexer.Errorf(t.Pos, "invalid quoted string %q: %s", t.Value, err.Error())
		}
		t.Value = value
		return t, nil
	}, types...)
}

func unquote(s string) (string, error) {
	quote := s[0]
	s = s[1 : len(s)-1]
	out := ""
	for s != "" {
		value, _, tail, err := strconv.UnquoteChar(s, quote)
		if err != nil {
			return "", err
		}
		s = tail
		out += string(value)
	}
	return out, nil
}

// Upper is an Option that upper-cases all tokens of the given type. Useful for case normalisation.
func Upper(types ...string) Option {
	return Map(func(token lexer.Token) (lexer.Token, error) {
		token.Value = strings.ToUpper(token.Value)
		return token, nil
	}, types...)
}

// Elide drops tokens of the specified types.
func Elide(types ...string) Option {
	return Map(func(token lexer.Token) (lexer.Token, error) {
		return lexer.Token{}, DropToken
	}, types...)
}

// Apply a Mapping to all tokens coming out of a Lexer.
type mappingLexerDef struct {
	lexer.Definition
	mapper Mapper
}

func (m *mappingLexerDef) Lex(r io.Reader) (lexer.Lexer, error) {
	lexer, err := m.Definition.Lex(r)
	if err != nil {
		return nil, err
	}
	return &mappingLexer{lexer, m.mapper}, nil
}

type mappingLexer struct {
	lexer.Lexer
	mapper Mapper
}

func (m *mappingLexer) Next() (lexer.Token, error) {
	for {
		t, err := m.Lexer.Next()
		if err != nil {
			return t, err
		}
		t, err = m.mapper(t)
		if err == DropToken {
			continue
		}
		return t, err
	}
}
