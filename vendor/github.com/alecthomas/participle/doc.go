// Package participle constructs parsers from definitions in struct tags and parses directly into
// those structs. The approach is philosophically similar to how other marshallers work in Go,
// "unmarshalling" an instance of a grammar into a struct.
//
// The supported annotation syntax is:
//
//		- `@<expr>` Capture expression into the field.
//		- `@@` Recursively capture using the fields own type.
//		- `<identifier>` Match named lexer token.
//		- `( ... )` Group.
//		- `"..."` Match the literal (note that the lexer must emit tokens matching this literal exactly).
//		- `"...":<identifier>` Match the literal, specifying the exact lexer token type to match.
//		- `<expr> <expr> ...` Match expressions.
//		- `<expr> | <expr>` Match one of the alternatives.
//
// The following modifiers can be used after any expression:
//
//		- `*` Expression can match zero or more times.
//		- `+` Expression must match one or more times.
//		- `?` Expression can match zero or once.
//		- `!` Require a non-empty match (this is useful with a sequence of optional matches eg. `("a"? "b"? "c"?)!`).
//
// Supported but deprecated:
//
//		- `{ ... }` Match 0 or more times (**DEPRECATED** - prefer `( ... )*`).
//		- `[ ... ]` Optional (**DEPRECATED** - prefer `( ... )?`).
//
// Here's an example of an EBNF grammar.
//
//     type Group struct {
//         Expression *Expression `"(" @@ ")"`
//     }
//
//     type Option struct {
//         Expression *Expression `"[" @@ "]"`
//     }
//
//     type Repetition struct {
//         Expression *Expression `"{" @@ "}"`
//     }
//
//     type Literal struct {
//         Start string `@String` // lexer.Lexer token "String"
//         End   string `("…" @String)?`
//     }
//
//     type Term struct {
//         Name       string      `  @Ident`
//         Literal    *Literal    `| @@`
//         Group      *Group      `| @@`
//         Option     *Option     `| @@`
//         Repetition *Repetition `| @@`
//     }
//
//     type Sequence struct {
//         Terms []*Term `@@+`
//     }
//
//     type Expression struct {
//         Alternatives []*Sequence `@@ ("|" @@)*`
//     }
//
//     type Expressions []*Expression
//
//     type Production struct {
//         Name        string      `@Ident "="`
//         Expressions Expressions `@@+ "."`
//     }
//
//     type EBNF struct {
//         Productions []*Production `@@*`
//     }
package participle
