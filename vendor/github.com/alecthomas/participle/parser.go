package participle

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/alecthomas/participle/lexer"
)

// A Parser for a particular grammar and lexer.
type Parser struct {
	root            node
	lex             lexer.Definition
	typ             reflect.Type
	useLookahead    int
	caseInsensitive map[string]bool
	mappers         []mapperByToken
}

// MustBuild calls Build(grammar, options...) and panics if an error occurs.
func MustBuild(grammar interface{}, options ...Option) *Parser {
	parser, err := Build(grammar, options...)
	if err != nil {
		panic(err)
	}
	return parser
}

// Build constructs a parser for the given grammar.
//
// If "Lexer()" is not provided as an option, a default lexer based on text/scanner will be used. This scans typical Go-
// like tokens.
//
// See documentation for details
func Build(grammar interface{}, options ...Option) (parser *Parser, err error) {
	// Configure Parser struct with defaults + options.
	p := &Parser{
		lex:             lexer.TextScannerLexer,
		caseInsensitive: map[string]bool{},
		useLookahead:    1,
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("nil Option passed, signature has changed; " +
				"if you intended to provide a custom Lexer, try participle.Build(grammar, participle.Lexer(lexer))")
		}
		if err = option(p); err != nil {
			return nil, err
		}
	}

	if len(p.mappers) > 0 {
		mappers := map[rune][]Mapper{}
		symbols := p.lex.Symbols()
		for _, mapper := range p.mappers {
			if len(mapper.symbols) == 0 {
				mappers[lexer.EOF] = append(mappers[lexer.EOF], mapper.mapper)
			} else {
				for _, symbol := range mapper.symbols {
					if rn, ok := symbols[symbol]; !ok {
						return nil, fmt.Errorf("mapper %#v uses unknown token %q", mapper, symbol)
					} else { // nolint: golint
						mappers[rn] = append(mappers[rn], mapper.mapper)
					}
				}
			}
		}
		p.lex = &mappingLexerDef{p.lex, func(t lexer.Token) (lexer.Token, error) {
			combined := make([]Mapper, 0, len(mappers[t.Type])+len(mappers[lexer.EOF]))
			combined = append(combined, mappers[lexer.EOF]...)
			combined = append(combined, mappers[t.Type]...)

			var err error
			for _, m := range combined {
				t, err = m(t)
				if err != nil {
					return t, err
				}
			}
			return t, nil
		}}
	}

	context := newGeneratorContext(p.lex)
	v := reflect.ValueOf(grammar)
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	p.typ = v.Type()
	p.root, err = context.parseType(p.typ)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Lex uses the parser's lexer to tokenise input.
func (p *Parser) Lex(r io.Reader) ([]lexer.Token, error) {
	lex, err := p.lex.Lex(r)
	if err != nil {
		return nil, err
	}
	return lexer.ConsumeAll(lex)
}

// Parse from r into grammar v which must be of the same type as the grammar passed to
// participle.Build().
func (p *Parser) Parse(r io.Reader, v interface{}) (err error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}
	var stream reflect.Value
	if rv.Kind() == reflect.Chan {
		stream = rv
		rt := rv.Type().Elem()
		rv = reflect.New(rt).Elem()
	}
	rt := rv.Type()
	if rt != p.typ {
		return fmt.Errorf("must parse into value of type %s not %T", p.typ, v)
	}
	baseLexer, err := p.lex.Lex(r)
	if err != nil {
		return err
	}
	lex := lexer.Upgrade(baseLexer)
	caseInsensitive := map[rune]bool{}
	for sym, rn := range p.lex.Symbols() {
		if p.caseInsensitive[sym] {
			caseInsensitive[rn] = true
		}
	}
	ctx, err := newParseContext(lex, p.useLookahead, caseInsensitive)
	if err != nil {
		return err
	}
	// If the grammar implements Parseable, use it.
	if parseable, ok := v.(Parseable); ok {
		return p.rootParseable(ctx, parseable)
	}
	if rt.Kind() != reflect.Ptr || rt.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("target must be a pointer to a struct, not %s", rt)
	}
	if stream.IsValid() {
		return p.parseStreaming(ctx, stream)
	}
	return p.parseOne(ctx, rv)
}

func (p *Parser) parseStreaming(ctx *parseContext, rv reflect.Value) error {
	t := rv.Type().Elem().Elem()
	for {
		if token, _ := ctx.Peek(0); token.EOF() {
			rv.Close()
			return nil
		}
		v := reflect.New(t)
		if err := p.parseInto(ctx, v); err != nil {
			return err
		}
		rv.Send(v)
	}
}

func (p *Parser) parseOne(ctx *parseContext, rv reflect.Value) error {
	err := p.parseInto(ctx, rv)
	if err != nil {
		return err
	}
	token, err := ctx.Peek(0)
	if err != nil {
		return err
	} else if !token.EOF() {
		return lexer.Errorf(token.Pos, "unexpected trailing token %q", token)
	}
	return nil
}

func (p *Parser) parseInto(ctx *parseContext, rv reflect.Value) error {
	if rv.IsNil() {
		return fmt.Errorf("target must be a non-nil pointer to a struct, but is a nil %s", rv.Type())
	}
	pv, err := p.root.Parse(ctx, rv.Elem())
	if len(pv) > 0 && pv[0].Type() == rv.Elem().Type() {
		rv.Elem().Set(reflect.Indirect(pv[0]))
	}
	if err != nil {
		return err
	}
	if pv == nil {
		token, _ := ctx.Peek(0)
		return lexer.Errorf(token.Pos, "invalid syntax")
	}
	return nil
}

func (p *Parser) rootParseable(lex lexer.PeekingLexer, parseable Parseable) error {
	peek, err := lex.Peek(0)
	if err != nil {
		return err
	}
	err = parseable.Parse(lex)
	if err == NextMatch {
		return lexer.Errorf(peek.Pos, "invalid syntax")
	}
	if err == nil && !peek.EOF() {
		return lexer.Errorf(peek.Pos, "unexpected token %q", peek)
	}
	return err
}

// ParseString is a convenience around Parse().
func (p *Parser) ParseString(s string, v interface{}) error {
	return p.Parse(strings.NewReader(s), v)
}

// ParseBytes is a convenience around Parse().
func (p *Parser) ParseBytes(b []byte, v interface{}) error {
	return p.Parse(bytes.NewReader(b), v)
}

// String representation of the grammar.
func (p *Parser) String() string {
	return stringern(p.root, 128)
}
