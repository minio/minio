package lexer

// Upgrade a Lexer to a PeekingLexer with arbitrary lookahead.
func Upgrade(lexer Lexer) PeekingLexer {
	if peeking, ok := lexer.(PeekingLexer); ok {
		return peeking
	}
	return &lookaheadLexer{Lexer: lexer}
}

type lookaheadLexer struct {
	Lexer
	peeked []Token
}

func (l *lookaheadLexer) Peek(n int) (Token, error) {
	for len(l.peeked) <= n {
		t, err := l.Lexer.Next()
		if err != nil {
			return Token{}, err
		}
		if t.EOF() {
			return t, nil
		}
		l.peeked = append(l.peeked, t)
	}
	return l.peeked[n], nil
}

func (l *lookaheadLexer) Next() (Token, error) {
	if len(l.peeked) > 0 {
		t := l.peeked[0]
		l.peeked = l.peeked[1:]
		return t, nil
	}
	return l.Lexer.Next()
}
