// Package lexer defines interfaces and implementations used by Participle to perform lexing.
//
// The primary interfaces are Definition and Lexer. There are three implementations of these
// interfaces:
//
// TextScannerLexer is based on text/scanner. This is the fastest, but least flexible, in that
// tokens are restricted to those supported by that package. It can scan about 5M tokens/second on a
// late 2013 15" MacBook Pro.
//
// The second lexer is constructed via the Regexp() function, mapping regexp capture groups
// to tokens. The complete input source is read into memory, so it is unsuitable for large inputs.
//
// The final lexer provided accepts a lexical grammar in EBNF. Each capitalised production is a
// lexical token supported by the resulting Lexer. This is very flexible, but a bit slower, scanning
// around 730K tokens/second on the same machine, though it is currently completely unoptimised.
// This could/should be converted to a table-based lexer.
//
// Lexer implementations must use Panic/Panicf to report errors.
package lexer
