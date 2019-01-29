package participle

import (
	"reflect"

	"github.com/alecthomas/participle/lexer"
)

type contextFieldSet struct {
	pos        lexer.Position
	strct      reflect.Value
	field      structLexerField
	fieldValue []reflect.Value
}

// Context for a single parse.
type parseContext struct {
	*rewinder
	lookahead       int
	caseInsensitive map[rune]bool
	apply           []*contextFieldSet
}

func newParseContext(lex lexer.Lexer, lookahead int, caseInsensitive map[rune]bool) (*parseContext, error) {
	rew, err := newRewinder(lex)
	if err != nil {
		return nil, err
	}
	return &parseContext{
		rewinder:        rew,
		caseInsensitive: caseInsensitive,
		lookahead:       lookahead,
	}, nil
}

// Defer adds a function to be applied once a branch has been picked.
func (p *parseContext) Defer(pos lexer.Position, strct reflect.Value, field structLexerField, fieldValue []reflect.Value) {
	p.apply = append(p.apply, &contextFieldSet{pos, strct, field, fieldValue})
}

// Apply deferred functions.
func (p *parseContext) Apply() error {
	for _, apply := range p.apply {
		if err := setField(apply.pos, apply.strct, apply.field, apply.fieldValue); err != nil {
			return err
		}
	}
	p.apply = nil
	return nil
}

// Branch accepts the branch as the correct branch.
func (p *parseContext) Accept(branch *parseContext) {
	p.apply = append(p.apply, branch.apply...)
	p.rewinder = branch.rewinder
}

// Branch starts a new lookahead branch.
func (p *parseContext) Branch() *parseContext {
	branch := &parseContext{}
	*branch = *p
	branch.apply = nil
	branch.rewinder = p.rewinder.Lookahead()
	return branch
}

// Stop returns true if parsing should terminate after the given "branch" failed to match.
func (p *parseContext) Stop(branch *parseContext) bool {
	if branch.cursor > p.cursor+p.lookahead {
		p.Accept(branch)
		return true
	}
	return false
}

type rewinder struct {
	cursor, limit int
	tokens        []lexer.Token
}

func newRewinder(lex lexer.Lexer) (*rewinder, error) {
	r := &rewinder{}
	for {
		t, err := lex.Next()
		if err != nil {
			return nil, err
		}
		if t.EOF() {
			break
		}
		r.tokens = append(r.tokens, t)
	}
	return r, nil
}

func (r *rewinder) Next() (lexer.Token, error) {
	if r.cursor >= len(r.tokens) {
		return lexer.EOFToken(lexer.Position{}), nil
	}
	r.cursor++
	return r.tokens[r.cursor-1], nil
}

func (r *rewinder) Peek(n int) (lexer.Token, error) {
	i := r.cursor + n
	if i >= len(r.tokens) {
		return lexer.EOFToken(lexer.Position{}), nil
	}
	return r.tokens[i], nil
}

// Lookahead returns a new rewinder usable for lookahead.
func (r *rewinder) Lookahead() *rewinder {
	clone := &rewinder{}
	*clone = *r
	clone.limit = clone.cursor
	return clone
}

// Keep this lookahead rewinder.
func (r *rewinder) Keep() {
	r.limit = 0
}
