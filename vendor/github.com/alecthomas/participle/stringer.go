package participle

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/alecthomas/participle/lexer"
)

type stringerVisitor struct {
	bytes.Buffer
	seen map[node]bool
}

func stringern(n node, depth int) string {
	v := &stringerVisitor{seen: map[node]bool{}}
	v.visit(n, depth, false)
	return v.String()
}

func stringer(n node) string {
	return stringern(n, 1)
}

func (s *stringerVisitor) visit(n node, depth int, disjunctions bool) {
	if s.seen[n] || depth <= 0 {
		fmt.Fprintf(s, "...")
		return
	}
	s.seen[n] = true

	switch n := n.(type) {
	case *disjunction:
		for i, c := range n.nodes {
			if i > 0 {
				fmt.Fprint(s, " | ")
			}
			s.visit(c, depth, disjunctions || len(n.nodes) > 1)
		}

	case *strct:
		s.visit(n.expr, depth, disjunctions)

	case *sequence:
		c := n
		for i := 0; c != nil && depth-i > 0; c, i = c.next, i+1 {
			if c != n {
				fmt.Fprint(s, " ")
			}
			s.visit(c.node, depth-i, disjunctions)
		}
		if c != nil {
			fmt.Fprint(s, " ...")
		}

	case *parseable:
		fmt.Fprintf(s, "<%s>", strings.ToLower(n.t.Name()))

	case *capture:
		if _, ok := n.node.(*parseable); ok {
			fmt.Fprintf(s, "<%s>", strings.ToLower(n.field.Name))
		} else {
			if n.node == nil {
				fmt.Fprintf(s, "<%s>", strings.ToLower(n.field.Name))
			} else {
				s.visit(n.node, depth, disjunctions)
			}
		}

	case *reference:
		fmt.Fprintf(s, "<%s>", strings.ToLower(n.identifier))

	case *optional:
		fmt.Fprint(s, "[ ")
		s.visit(n.node, depth, disjunctions)
		fmt.Fprint(s, " ]")

	case *repetition:
		fmt.Fprint(s, "{ ")
		s.visit(n.node, depth, disjunctions)
		fmt.Fprint(s, " }")

	case *literal:
		fmt.Fprintf(s, "%q", n.s)
		if n.t != lexer.EOF && n.s == "" {
			fmt.Fprintf(s, ":%s", n.tt)
		}

	case *group:
		fmt.Fprint(s, "(")
		if child, ok := n.expr.(*group); ok && child.mode == groupMatchOnce {
			s.visit(child.expr, depth, disjunctions)
		} else if child, ok := n.expr.(*capture); ok {
			if grandchild, ok := child.node.(*group); ok && grandchild.mode == groupMatchOnce {
				s.visit(grandchild.expr, depth, disjunctions)
			} else {
				s.visit(n.expr, depth, disjunctions)
			}
		} else {
			s.visit(n.expr, depth, disjunctions)
		}
		fmt.Fprint(s, ")")
		switch n.mode {
		case groupMatchNonEmpty:
			fmt.Fprintf(s, "!")
		case groupMatchZeroOrOne:
			fmt.Fprintf(s, "?")
		case groupMatchZeroOrMore:
			fmt.Fprintf(s, "*")
		case groupMatchOneOrMore:
			fmt.Fprintf(s, "+")
		}

	default:
		panic("unsupported")
	}
}
