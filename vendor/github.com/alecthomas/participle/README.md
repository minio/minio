# A dead simple parser package for Go

[![Godoc](https://godoc.org/github.com/alecthomas/participle?status.svg)](http://godoc.org/github.com/alecthomas/participle) [![CircleCI](https://img.shields.io/circleci/project/github/alecthomas/participle.svg)](https://circleci.com/gh/alecthomas/participle)
 [![Go Report Card](https://goreportcard.com/badge/github.com/alecthomas/participle)](https://goreportcard.com/report/github.com/alecthomas/participle) [![Gitter chat](https://badges.gitter.im/alecthomas.png)](https://gitter.im/alecthomas/Lobby)

<!-- TOC -->

1. [Introduction](#introduction)
2. [Limitations](#limitations)
3. [Tutorial](#tutorial)
4. [Overview](#overview)
5. [Annotation syntax](#annotation-syntax)
6. [Capturing](#capturing)
7. [Streaming](#streaming)
8. [Lexing](#lexing)
9. [Options](#options)
10. [Examples](#examples)
11. [Performance](#performance)

<!-- /TOC -->

<a id="markdown-introduction" name="introduction"></a>
## Introduction

The goal of this package is to provide a simple, idiomatic and elegant way of
defining parsers in Go.

Participle's method of defining grammars should be familiar to any Go
programmer who has used the `encoding/json` package: struct field tags define
what and how input is mapped to those same fields. This is not unusual for Go
encoders, but is unusual for a parser.

<a id="markdown-limitations" name="limitations"></a>
## Limitations

Participle parsers are recursive descent. Among other things, this means that they do not support left recursion.

There is an experimental lookahead option for using precomputed lookahead
tables for disambiguation. You can enable this with the parser option
`participle.UseLookahead()`.

Left recursion must be eliminated by restructuring your grammar.

<a id="markdown-tutorial" name="tutorial"></a>
## Tutorial

A [tutorial](TUTORIAL.md) is available, walking through the creation of an .ini parser.

<a id="markdown-overview" name="overview"></a>
## Overview

A grammar is an annotated Go structure used to both define the parser grammar,
and be the AST output by the parser. As an example, following is the final INI
parser from the tutorial.

 ```go
 type INI struct {
   Properties []*Property `{ @@ }`
   Sections   []*Section  `{ @@ }`
 }

 type Section struct {
   Identifier string      `"[" @Ident "]"`
   Properties []*Property `{ @@ }`
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

> **Note:** Participle also supports named struct tags (eg. <code>Hello string &#96;parser:"@Ident"&#96;</code>).

A parser is constructed from a grammar and a lexer:

```go
parser, err := participle.Build(&INI{})
```

Once constructed, the parser is applied to input to produce an AST:

```go
ast := &INI{}
err := parser.ParseString("size = 10", ast)
// ast == &INI{
//   Properties: []*Property{
//     {Key: "size", Value: &Value{Number: &10}},
//   },
// }
```

<a id="markdown-annotation-syntax" name="annotation-syntax"></a>
## Annotation syntax

- `@<expr>` Capture expression into the field.
- `@@` Recursively capture using the fields own type.
- `<identifier>` Match named lexer token.
- `( ... )` Group.
- `"..."` Match the literal (note that the lexer must emit tokens matching this literal exactly).
- `"...":<identifier>` Match the literal, specifying the exact lexer token type to match.
- `<expr> <expr> ...` Match expressions.
- `<expr> | <expr>` Match one of the alternatives.

The following modifiers can be used after any expression:

- `*` Expression can match zero or more times.
- `+` Expression must match one or more times.
- `?` Expression can match zero or once.
- `!` Require a non-empty match (this is useful with a sequence of optional matches eg. `("a"? "b"? "c"?)!`).

Supported but deprecated:
- `{ ... }` Match 0 or more times (**DEPRECATED** - prefer `( ... )*`).
- `[ ... ]` Optional (**DEPRECATED** - prefer `( ... )?`).

Notes:

- Each struct is a single production, with each field applied in sequence.
- `@<expr>` is the mechanism for capturing matches into the field.
- if a struct field is not keyed with "parser", the entire struct tag
  will be used as the grammar fragment. This allows the grammar syntax to remain
  clear and simple to maintain.

<a id="markdown-capturing" name="capturing"></a>
## Capturing

Prefixing any expression in the grammar with `@` will capture matching values
for that expression into the corresponding field.

For example:

```go
// The grammar definition.
type Grammar struct {
  Hello string `@Ident`
}

// The source text to parse.
source := "world"

// After parsing, the resulting AST.
result == &Grammar{
  Hello: "world",
}
```

For slice and string fields, each instance of `@` will accumulate into the
field (including repeated patterns). Accumulation into other types is not
supported.

A successful capture match into a boolean field will set the field to true.

For integer and floating point types, a successful capture will be parsed
with `strconv.ParseInt()` and `strconv.ParseBool()` respectively.

Custom control of how values are captured into fields can be achieved by a
field type implementing the `Capture` interface (`Capture(values []string)
error`).

<a id="markdown-streaming" name="streaming"></a>
## Streaming

Participle supports streaming parsing. Simply pass a channel of your grammar into
`Parse*()`. The grammar will be repeatedly parsed and sent to the channel. Note that
the `Parse*()` call will not return until parsing completes, so it should generally be
started in a goroutine.

```go
type token struct {
  Str string `  @Ident`
  Num int    `| @Int`
}

parser, err := participle.Build(&token{})

tokens := make(chan *token, 128)
err := parser.ParseString(`hello 10 11 12 world`, tokens)
for token := range tokens {
  fmt.Printf("%#v\n", token)
}
```

<a id="markdown-lexing" name="lexing"></a>
## Lexing

Participle operates on tokens and thus relies on a lexer to convert character
streams to tokens.

Three lexers are provided, varying in speed and flexibility. The fastest lexer
is based on the [text/scanner](https://golang.org/pkg/text/scanner/) package
but only allows tokens provided by that package. Next fastest is the regexp
lexer (`lexer.Regexp()`). The slowest is currently the EBNF based lexer, but it has a large potential for optimisation through code generation.

To use your own Lexer you will need to implement two interfaces:
[Definition](https://godoc.org/github.com/alecthomas/participle/lexer#Definition)
and [Lexer](https://godoc.org/github.com/alecthomas/participle/lexer#Lexer).

<a id="markdown-options" name="options"></a>
## Options

The Parser's behaviour can be configured via [Options](https://godoc.org/github.com/alecthomas/participle#Option).

<a id="markdown-examples" name="examples"></a>
## Examples

There are several [examples](https://github.com/alecthomas/participle/tree/master/_examples) included:

Example | Description
--------|---------------
[BASIC](https://github.com/alecthomas/participle/tree/master/_examples/basic) | A lexer, parser and interpreter for a [rudimentary dialect](https://caml.inria.fr/pub/docs/oreilly-book/html/book-ora058.html) of BASIC.
[EBNF](https://github.com/alecthomas/participle/tree/master/_examples/ebnf) | Parser for the form of EBNF used by Go.
[Expr](https://github.com/alecthomas/participle/tree/master/_examples/expr) | A basic mathematical expression parser and evaluator.
[GraphQL](https://github.com/alecthomas/participle/tree/master/_examples/graphql) | Lexer+parser for GraphQL schemas
[HCL](https://github.com/alecthomas/participle/tree/master/_examples/hcl) | A parser for the [HashiCorp Configuration Language](https://github.com/hashicorp/hcl).
[INI](https://github.com/alecthomas/participle/tree/master/_examples/ini) | An INI file parser.
[Protobuf](https://github.com/alecthomas/participle/tree/master/_examples/protobuf) | A full [Protobuf](https://developers.google.com/protocol-buffers/) version 2 and 3 parser.
[SQL](https://github.com/alecthomas/participle/tree/master/_examples/sql) | A *very* rudimentary SQL SELECT parser.
[Thrift](https://github.com/alecthomas/participle/tree/master/_examples/thrift) | A full [Thrift](https://thrift.apache.org/docs/idl) parser.
[TOML](https://github.com/alecthomas/participle/blob/master/_examples/toml/main.go) | A [TOML](https://github.com/toml-lang/toml) parser.

Included below is a full GraphQL lexer and parser:

```go
package main

import (
  "os"

  "github.com/alecthomas/kong"
  "github.com/alecthomas/repr"

  "github.com/alecthomas/participle"
  "github.com/alecthomas/participle/lexer"
  "github.com/alecthomas/participle/lexer/ebnf"
)

type File struct {
  Entries []*Entry `{ @@ }`
}

type Entry struct {
  Type   *Type   `  @@`
  Schema *Schema `| @@`
  Enum   *Enum   `| @@`
  Scalar string  `| "scalar" @Ident`
}

type Enum struct {
  Name  string   `"enum" @Ident`
  Cases []string `"{" { @Ident } "}"`
}

type Schema struct {
  Fields []*Field `"schema" "{" { @@ } "}"`
}

type Type struct {
  Name       string   `"type" @Ident`
  Implements string   `[ "implements" @Ident ]`
  Fields     []*Field `"{" { @@ } "}"`
}

type Field struct {
  Name       string      `@Ident`
  Arguments  []*Argument `[ "(" [ @@ { "," @@ } ] ")" ]`
  Type       *TypeRef    `":" @@`
  Annotation string      `[ "@" @Ident ]`
}

type Argument struct {
  Name    string   `@Ident`
  Type    *TypeRef `":" @@`
  Default *Value   `[ "=" @@ ]`
}

type TypeRef struct {
  Array       *TypeRef `(   "[" @@ "]"`
  Type        string   `  | @Ident )`
  NonNullable bool     `[ @"!" ]`
}

type Value struct {
  Symbol string `@Ident`
}

var (
  graphQLLexer = lexer.Must(ebnf.New(`
    Comment = ("#" | "//") { "\u0000"…"\uffff"-"\n" } .
    Ident = (alpha | "_") { "_" | alpha | digit } .
    Number = ("." | digit) {"." | digit} .
    Whitespace = " " | "\t" | "\n" | "\r" .
    Punct = "!"…"/" | ":"…"@" | "["…`+"\"`\""+` | "{"…"~" .

    alpha = "a"…"z" | "A"…"Z" .
    digit = "0"…"9" .
`))

  parser = participle.MustBuild(&File{},
    participle.Lexer(graphQLLexer),
    participle.Elide("Comment", "Whitespace"),
    )

  cli struct {
    Files []string `arg:"" type:"existingfile" required:"" help:"GraphQL schema files to parse."`
  }
)

func main() {
  ctx := kong.Parse(&cli)
  for _, file := range cli.Files {
    ast := &File{}
    r, err := os.Open(file)
    ctx.FatalIfErrorf(err)
    err = parser.Parse(r, ast)
    r.Close()
    repr.Println(ast)
    ctx.FatalIfErrorf(err)
  }
}

```

<a id="markdown-performance" name="performance"></a>
## Performance

One of the included examples is a complete Thrift parser
(shell-style comments are not supported). This gives
a convenient baseline for comparing to the PEG based
[pigeon](https://github.com/PuerkitoBio/pigeon), which is the parser used by
[go-thrift](https://github.com/samuel/go-thrift). Additionally, the pigeon
parser is utilising a generated parser, while the participle parser is built at
run time.

You can run the benchmarks yourself, but here's the output on my machine:

    BenchmarkParticipleThrift-4        10000      221818 ns/op     48880 B/op     1240 allocs/op
    BenchmarkGoThriftParser-4           2000      804709 ns/op    170301 B/op     3086 allocs/op

On a real life codebase of 47K lines of Thrift, Participle takes 200ms and go-
thrift takes 630ms, which aligns quite closely with the benchmarks.
