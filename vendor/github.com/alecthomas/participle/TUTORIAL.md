# Participle parser tutorial

<!-- MarkdownTOC -->

1. [Introduction](#introduction)
1. [The complete grammar](#the-complete-grammar)
1. [Root of the .ini AST \(structure, fields\)](#root-of-the-ini-ast-structure-fields)
1. [.ini properties \(named tokens, capturing, literals\)](#ini-properties-named-tokens-capturing-literals)
1. [.ini property values \(alternates, recursive structs, sequences\)](#ini-property-values-alternates-recursive-structs-sequences)
1. [Complete, but limited, .ini grammar \(top-level properties only\)](#complete-but-limited-ini-grammar-top-level-properties-only)
1. [Extending our grammar to support sections](#extending-our-grammar-to-support-sections)
1. [\(Optional\) Source positional information](#optional-source-positional-information)
1. [Parsing using our grammar](#parsing-using-our-grammar)

<!-- /MarkdownTOC -->

## Introduction

Writing a parser in Participle typically involves starting from the "root" of
the AST, annotating fields with the grammar, then recursively expanding until
it is complete. The AST is expressed via Go data types and the grammar is
expressed through struct field tags, as a form of EBNF.

The parser we're going to create for this tutorial parses .ini files
like this:

```ini
age = 21
name = "Bob Smith"

[address]
city = "Beverly Hills"
postal_code = 90210
```

## The complete grammar

I think it's useful to see the complete grammar first, to see what we're
working towards. Read on below for details.

 ```go
 type INI struct {
   Properties []*Property `@@*`
   Sections   []*Section  `@@*`
 }

 type Section struct {
   Identifier string      `"[" @Ident "]"`
   Properties []*Property `@@*`
 }

 type Property struct {
   Key   string `@Ident "="`
   Value *Value `@@`
 }

 type Value struct {
   String *string  `  @String`
   Number *float64 `| @Float`
 }
 ```

## Root of the .ini AST (structure, fields)

The first step is to create a root struct for our grammar. In the case of our
.ini parser, this struct will contain a sequence of properties:

```go
type INI struct {
  Properties []*Property
}

type Property struct {
}
```

## .ini properties (named tokens, capturing, literals)

Each property in an .ini file has an identifier key:

```go
type Property struct {
  Key string
}
```

The default lexer tokenises Go source code, and includes an `Ident` token type
that matches identifiers. To match this token we simply use the token type
name:

```go
type Property struct {
  Key string `Ident`
}
```

This will *match* identifiers, but not *capture* them into the `Key` field. To
capture input tokens into AST fields, prefix any grammar node with `@`:

```go
type Property struct {
  Key string `@Ident`
}
```

In .ini files, each key is separated from its value with a literal `=`. To
match a literal, enclose the literal in double quotes:

```go
type Property struct {
  Key string `@Ident "="`
}
```

> Note: literals in the grammar must match tokens from the lexer *exactly*. In
> this example if the lexer does not output `=` as a distinct token the
> grammar will not match.

## .ini property values (alternates, recursive structs, sequences)

For the purposes of our example we are only going to support quoted string
and numeric property values. As each value can be *either* a string or a float
we'll need something akin to a sum type. Go's type system cannot express this
directly, so we'll use the common approach of making each element a pointer.
The selected "case" will *not* be nil.

```go
type Value struct {
  String *string
  Number *float64
}
```

> Note: Participle will hydrate pointers as necessary.

To express matching a set of alternatives we use the `|` operator:

```go
type Value struct {
  String *string  `  @String`
  Number *float64 `| @Float`
}
```

> Note: the grammar can cross fields.

Next, we'll match values and capture them into the `Property`. To recursively
capture structs use `@@` (capture self):

```go
type Property struct {
  Key   string `@Ident "="`
  Value *Value `@@`
}
```

Now that we can parse a `Property` we need to go back to the root of the
grammar. We want to parse 0 or more properties. To do this, we use `<expr>*`.
Participle will accumulate each match into the slice until matching fails,
then move to the next node in the grammar.

```go
type INI struct {
  Properties []*Property `@@*`
}
```

> Note: tokens can also be accumulated into strings, appending each match.

## Complete, but limited, .ini grammar (top-level properties only)

We now have a functional, but limited, .ini parser!

```go
type INI struct {
  Properties []*Property `@@*`
}

type Property struct {
  Key string   `@Ident "="`
  Value *Value `@@`
}

type Value struct {
  String *string  `  @String`
  Number *float64 `| @Float`
}
```

## Extending our grammar to support sections

Adding support for sections is simply a matter of utilising the constructs
we've just learnt. A section consists of a header identifier, and a sequence
of properties:

```go
type Section struct {
  Identifier string      `"[" @Ident "]"`
  Properties []*Property `@@*`
}
```

Simple!

Now we just add a sequence of `Section`s to our root node:

```go
type INI struct {
  Properties []*Property `@@*`
  Sections   []*Section  `@@*`
}
```

And we're done!

## (Optional) Source positional information

If a grammar node includes a field with the name `Pos` and type `lexer.Position`, it will be automatically populated by positional information. eg.

```go
type Value struct {
  Pos    lexer.Position
  String *string  `  @String`
  Number *float64 `| @Float`
}
```

This is useful for error reporting.

## Parsing using our grammar

To parse with this grammar we first construct the parser (we'll use the
default lexer for now):

```go
parser, err := participle.Build(&INI{})
```

Then create a root node and parse into it with `parser.Parse{,String,Bytes}()`:

```go
ini := &INI{}
err = parser.ParseString(`
age = 21
name = "Bob Smith"

[address]
city = "Beverly Hills"
postal_code = 90210
`, ini)
```

You can find the full example [here](_examples/ini/main.go), alongside
other examples including an SQL `SELECT` parser and a full
[Thrift](https://thrift.apache.org/) parser.
