package participle

import (
	"fmt"
	"reflect"

	"github.com/alecthomas/participle/lexer"
)

// A structLexer lexes over the tags of struct fields while tracking the current field.
type structLexer struct {
	s       reflect.Type
	field   int
	indexes [][]int
	lexer   lexer.PeekingLexer
}

func lexStruct(s reflect.Type) (*structLexer, error) {
	indexes, err := collectFieldIndexes(s)
	if err != nil {
		return nil, err
	}
	slex := &structLexer{
		s:       s,
		indexes: indexes,
	}
	if len(slex.indexes) > 0 {
		tag := fieldLexerTag(slex.Field().StructField)
		slex.lexer = lexer.Upgrade(lexer.LexString(tag))
	}
	return slex, nil
}

// NumField returns the number of fields in the struct associated with this structLexer.
func (s *structLexer) NumField() int {
	return len(s.indexes)
}

type structLexerField struct {
	reflect.StructField
	Index []int
}

// Field returns the field associated with the current token.
func (s *structLexer) Field() structLexerField {
	return s.GetField(s.field)
}

func (s *structLexer) GetField(field int) structLexerField {
	if field >= len(s.indexes) {
		field = len(s.indexes) - 1
	}
	return structLexerField{
		StructField: s.s.FieldByIndex(s.indexes[field]),
		Index:       s.indexes[field],
	}
}

func (s *structLexer) Peek() (lexer.Token, error) {
	field := s.field
	lex := s.lexer
	for {
		token, err := lex.Peek(0)
		if err != nil {
			return token, err
		}
		if !token.EOF() {
			token.Pos.Line = field + 1
			return token, nil
		}
		field++
		if field >= s.NumField() {
			return lexer.EOFToken(token.Pos), nil
		}
		tag := fieldLexerTag(s.GetField(field).StructField)
		lex = lexer.Upgrade(lexer.LexString(tag))
	}
}

func (s *structLexer) Next() (lexer.Token, error) {
	token, err := s.lexer.Next()
	if err != nil {
		return token, err
	}
	if !token.EOF() {
		token.Pos.Line = s.field + 1
		return token, nil
	}
	if s.field+1 >= s.NumField() {
		return lexer.EOFToken(token.Pos), nil
	}
	s.field++
	tag := fieldLexerTag(s.Field().StructField)
	s.lexer = lexer.Upgrade(lexer.LexString(tag))
	return s.Next()
}

func fieldLexerTag(field reflect.StructField) string {
	if tag, ok := field.Tag.Lookup("parser"); ok {
		return tag
	}
	return string(field.Tag)
}

// Recursively collect flattened indices for top-level fields and embedded fields.
func collectFieldIndexes(s reflect.Type) (out [][]int, err error) {
	if s.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a struct but got %q", s)
	}
	defer decorate(&err, s.String)
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if f.Anonymous {
			children, err := collectFieldIndexes(f.Type)
			if err != nil {
				return nil, err
			}
			for _, idx := range children {
				out = append(out, append(f.Index, idx...))
			}
		} else if fieldLexerTag(f) != "" {
			out = append(out, f.Index)
		}
	}
	return
}
