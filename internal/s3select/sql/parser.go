// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package sql

import (
	"strings"

	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

// Types with custom Capture interface for parsing

// Boolean is a type for a parsed Boolean literal
type Boolean bool

// Capture interface used by participle
func (b *Boolean) Capture(values []string) error {
	*b = strings.ToLower(values[0]) == "true"
	return nil
}

// LiteralString is a type for parsed SQL string literals
type LiteralString string

// Capture interface used by participle
func (ls *LiteralString) Capture(values []string) error {
	// Remove enclosing single quote
	n := len(values[0])
	r := values[0][1 : n-1]
	// Translate doubled quotes
	*ls = LiteralString(strings.Replace(r, "''", "'", -1))
	return nil
}

// LiteralList is a type for parsed SQL lists literals
type LiteralList []string

// Capture interface used by participle
func (ls *LiteralList) Capture(values []string) error {
	// Remove enclosing parenthesis.
	n := len(values[0])
	r := values[0][1 : n-1]
	// Translate doubled quotes
	*ls = LiteralList(strings.Split(r, ","))
	return nil
}

// ObjectKey is a type for parsed strings occurring in key paths
type ObjectKey struct {
	Lit *LiteralString `parser:" \"[\" @LitString \"]\""`
	ID  *Identifier    `parser:"| \".\" @@"`
}

// QuotedIdentifier is a type for parsed strings that are double
// quoted.
type QuotedIdentifier string

// Capture interface used by participle
func (qi *QuotedIdentifier) Capture(values []string) error {
	// Remove enclosing quotes
	n := len(values[0])
	r := values[0][1 : n-1]

	// Translate doubled quotes
	*qi = QuotedIdentifier(strings.Replace(r, `""`, `"`, -1))
	return nil
}

// Types representing AST of SQL statement. Only SELECT is supported.

// Select is the top level AST node type
type Select struct {
	Expression *SelectExpression `parser:"\"SELECT\" @@"`
	From       *TableExpression  `parser:"\"FROM\" @@"`
	Where      *Expression       `parser:"( \"WHERE\" @@ )?"`
	Limit      *LitValue         `parser:"( \"LIMIT\" @@ )?"`
}

// SelectExpression represents the items requested in the select
// statement
type SelectExpression struct {
	All         bool                 `parser:"  @\"*\""`
	Expressions []*AliasedExpression `parser:"| @@ { \",\" @@ }"`
}

// TableExpression represents the FROM clause
type TableExpression struct {
	Table *JSONPath `parser:"@@"`
	As    string    `parser:"( \"AS\"? @Ident )?"`
}

// JSONPathElement represents a keypath component
type JSONPathElement struct {
	Key            *ObjectKey `parser:"  @@"`               // ['name'] and .name forms
	Index          *int       `parser:"| \"[\" @Int \"]\""` // [3] form
	ObjectWildcard bool       `parser:"| @\".*\""`          // .* form
	ArrayWildcard  bool       `parser:"| @\"[*]\""`         // [*] form
}

// JSONPath represents a keypath.
// Instances should be treated idempotent and not change once created.
type JSONPath struct {
	BaseKey  *Identifier        `parser:" @@"`
	PathExpr []*JSONPathElement `parser:"(@@)*"`

	// Cached values:
	pathString         string
	strippedTableAlias string
	strippedPathExpr   []*JSONPathElement
}

// AliasedExpression is an expression that can be optionally named
type AliasedExpression struct {
	Expression *Expression `parser:"@@"`
	As         string      `parser:"[ \"AS\" @Ident ]"`
}

// Grammar for Expression
//
// Expression          → AndCondition ("OR" AndCondition)*
// AndCondition        → Condition ("AND" Condition)*
// Condition           → "NOT" Condition | ConditionExpression
// ConditionExpression → ValueExpression ("=" | "<>" | "<=" | ">=" | "<" | ">") ValueExpression
//                     | ValueExpression "LIKE" ValueExpression ("ESCAPE" LitString)?
//                     | ValueExpression ("NOT"? "BETWEEN" ValueExpression "AND" ValueExpression)
//                     | ValueExpression "IN" "(" Expression ("," Expression)* ")"
//                     | ValueExpression
// ValueExpression     → Operand
//
// Operand grammar follows below

// Expression represents a logical disjunction of clauses
type Expression struct {
	And []*AndCondition `parser:"@@ ( \"OR\" @@ )*"`
}

// ListExpr represents a literal list with elements as expressions.
type ListExpr struct {
	Elements []*Expression `parser:"\"(\" @@ ( \",\" @@ )* \")\" | \"[\" @@ ( \",\" @@ )* \"]\""`
}

// AndCondition represents logical conjunction of clauses
type AndCondition struct {
	Condition []*Condition `parser:"@@ ( \"AND\" @@ )*"`
}

// Condition represents a negation or a condition operand
type Condition struct {
	Operand *ConditionOperand `parser:"  @@"`
	Not     *Condition        `parser:"| \"NOT\" @@"`
}

// ConditionOperand is a operand followed by an an optional operation
// expression
type ConditionOperand struct {
	Operand      *Operand      `parser:"@@"`
	ConditionRHS *ConditionRHS `parser:"@@?"`
}

// ConditionRHS represents the right-hand-side of Compare, Between, In
// or Like expressions.
type ConditionRHS struct {
	Compare *Compare `parser:"  @@"`
	Between *Between `parser:"| @@"`
	In      *In      `parser:"| \"IN\" @@"`
	Like    *Like    `parser:"| @@"`
}

// Compare represents the RHS of a comparison expression
type Compare struct {
	Operator string   `parser:"@( \"<>\" | \"<=\" | \">=\" | \"=\" | \"<\" | \">\" | \"!=\" )"`
	Operand  *Operand `parser:"  @@"`
}

// Like represents the RHS of a LIKE expression
type Like struct {
	Not        bool     `parser:" @\"NOT\"? "`
	Pattern    *Operand `parser:" \"LIKE\" @@ "`
	EscapeChar *Operand `parser:" (\"ESCAPE\" @@)? "`
}

// Between represents the RHS of a BETWEEN expression
type Between struct {
	Not   bool     `parser:" @\"NOT\"? "`
	Start *Operand `parser:" \"BETWEEN\" @@ "`
	End   *Operand `parser:" \"AND\" @@ "`
}

// In represents the RHS of an IN expression
type In struct {
	ListExpression *Expression `parser:"@@ "`
}

// Grammar for Operand:
//
// operand → multOp ( ("-" | "+") multOp )*
// multOp  → unary ( ("/" | "*" | "%") unary )*
// unary   → "-" unary | primary
// primary → Value | Variable | "(" expression ")"
//

// An Operand is a single term followed by an optional sequence of
// terms separated by +/-
type Operand struct {
	Left  *MultOp     `parser:"@@"`
	Right []*OpFactor `parser:"(@@)*"`
}

// OpFactor represents the right-side of a +/- operation.
type OpFactor struct {
	Op    string  `parser:"@(\"+\" | \"-\")"`
	Right *MultOp `parser:"@@"`
}

// MultOp represents a single term followed by an optional sequence of
// terms separated by *, / or % operators.
type MultOp struct {
	Left  *UnaryTerm     `parser:"@@"`
	Right []*OpUnaryTerm `parser:"(@@)*"`
}

// OpUnaryTerm represents the right side of *, / or % binary operations.
type OpUnaryTerm struct {
	Op    string     `parser:"@(\"*\" | \"/\" | \"%\")"`
	Right *UnaryTerm `parser:"@@"`
}

// UnaryTerm represents a single negated term or a primary term
type UnaryTerm struct {
	Negated *NegatedTerm `parser:"  @@"`
	Primary *PrimaryTerm `parser:"| @@"`
}

// NegatedTerm has a leading minus sign.
type NegatedTerm struct {
	Term *PrimaryTerm `parser:"\"-\" @@"`
}

// PrimaryTerm represents a Value, Path expression, a Sub-expression
// or a function call.
type PrimaryTerm struct {
	Value         *LitValue   `parser:"  @@"`
	JPathExpr     *JSONPath   `parser:"| @@"`
	ListExpr      *ListExpr   `parser:"| @@"`
	SubExpression *Expression `parser:"| \"(\" @@ \")\""`
	// Include function expressions here.
	FuncCall *FuncExpr `parser:"| @@"`
}

// FuncExpr represents a function call
type FuncExpr struct {
	SFunc     *SimpleArgFunc `parser:"  @@"`
	Count     *CountFunc     `parser:"| @@"`
	Cast      *CastFunc      `parser:"| @@"`
	Substring *SubstringFunc `parser:"| @@"`
	Extract   *ExtractFunc   `parser:"| @@"`
	Trim      *TrimFunc      `parser:"| @@"`
	DateAdd   *DateAddFunc   `parser:"| @@"`
	DateDiff  *DateDiffFunc  `parser:"| @@"`

	// Used during evaluation for aggregation funcs
	aggregate *aggVal
}

// SimpleArgFunc represents functions with simple expression
// arguments.
type SimpleArgFunc struct {
	FunctionName string `parser:" @(\"AVG\" | \"MAX\" | \"MIN\" | \"SUM\" |  \"COALESCE\" | \"NULLIF\" | \"TO_STRING\" | \"TO_TIMESTAMP\" | \"UTCNOW\" | \"CHAR_LENGTH\" | \"CHARACTER_LENGTH\" | \"LOWER\" | \"UPPER\") "`

	ArgsList []*Expression `parser:"\"(\" (@@ (\",\" @@)*)?\")\""`
}

// CountFunc represents the COUNT sql function
type CountFunc struct {
	StarArg bool        `parser:" \"COUNT\" \"(\" ( @\"*\"?"`
	ExprArg *Expression `parser:" @@? )! \")\""`
}

// CastFunc represents CAST sql function
type CastFunc struct {
	Expr     *Expression `parser:" \"CAST\" \"(\" @@ "`
	CastType string      `parser:" \"AS\" @(\"BOOL\" | \"INT\" | \"INTEGER\" | \"STRING\" | \"FLOAT\" | \"DECIMAL\" | \"NUMERIC\" | \"TIMESTAMP\") \")\" "`
}

// SubstringFunc represents SUBSTRING sql function
type SubstringFunc struct {
	Expr *PrimaryTerm `parser:" \"SUBSTRING\" \"(\" @@ "`
	From *Operand     `parser:" ( \"FROM\" @@ "`
	For  *Operand     `parser:"   (\"FOR\" @@)? \")\" "`
	Arg2 *Operand     `parser:" | \",\" @@ "`
	Arg3 *Operand     `parser:"   (\",\" @@)? \")\" )"`
}

// ExtractFunc represents EXTRACT sql function
type ExtractFunc struct {
	Timeword string       `parser:" \"EXTRACT\" \"(\" @( \"YEAR\":Timeword | \"MONTH\":Timeword | \"DAY\":Timeword | \"HOUR\":Timeword | \"MINUTE\":Timeword | \"SECOND\":Timeword | \"TIMEZONE_HOUR\":Timeword | \"TIMEZONE_MINUTE\":Timeword ) "`
	From     *PrimaryTerm `parser:" \"FROM\" @@ \")\" "`
}

// TrimFunc represents TRIM sql function
type TrimFunc struct {
	TrimWhere *string      `parser:" \"TRIM\" \"(\" ( @( \"LEADING\" | \"TRAILING\" | \"BOTH\" ) "`
	TrimChars *PrimaryTerm `parser:"             @@?  "`
	TrimFrom  *PrimaryTerm `parser:"             \"FROM\" )? @@ \")\" "`
}

// DateAddFunc represents the DATE_ADD function
type DateAddFunc struct {
	DatePart  string       `parser:" \"DATE_ADD\" \"(\" @( \"YEAR\":Timeword | \"MONTH\":Timeword | \"DAY\":Timeword | \"HOUR\":Timeword | \"MINUTE\":Timeword | \"SECOND\":Timeword ) \",\""`
	Quantity  *Operand     `parser:" @@ \",\""`
	Timestamp *PrimaryTerm `parser:" @@ \")\""`
}

// DateDiffFunc represents the DATE_DIFF function
type DateDiffFunc struct {
	DatePart   string       `parser:" \"DATE_DIFF\" \"(\" @( \"YEAR\":Timeword | \"MONTH\":Timeword | \"DAY\":Timeword | \"HOUR\":Timeword | \"MINUTE\":Timeword | \"SECOND\":Timeword ) \",\" "`
	Timestamp1 *PrimaryTerm `parser:" @@ \",\" "`
	Timestamp2 *PrimaryTerm `parser:" @@ \")\" "`
}

// LitValue represents a literal value parsed from the sql
type LitValue struct {
	Float   *float64       `parser:"(  @Float"`
	Int     *float64       `parser:" | @Int"` // To avoid value out of range, use float64 instead
	String  *LiteralString `parser:" | @LitString"`
	Boolean *Boolean       `parser:" | @(\"TRUE\" | \"FALSE\")"`
	Null    bool           `parser:" | @\"NULL\")"`
}

// Identifier represents a parsed identifier
type Identifier struct {
	Unquoted *string           `parser:"  @Ident"`
	Quoted   *QuotedIdentifier `parser:"| @QuotIdent"`
}

var (
	sqlLexer = lexer.Must(lexer.Regexp(`(\s+)` +
		`|(?P<Timeword>(?i)\b(?:YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|TIMEZONE_HOUR|TIMEZONE_MINUTE)\b)` +
		`|(?P<Keyword>(?i)\b(?:SELECT|FROM|TOP|DISTINCT|ALL|WHERE|GROUP|BY|HAVING|UNION|MINUS|EXCEPT|INTERSECT|ORDER|LIMIT|OFFSET|TRUE|FALSE|NULL|IS|NOT|ANY|SOME|BETWEEN|AND|OR|LIKE|ESCAPE|AS|IN|BOOL|INT|INTEGER|STRING|FLOAT|DECIMAL|NUMERIC|TIMESTAMP|AVG|COUNT|MAX|MIN|SUM|COALESCE|NULLIF|CAST|DATE_ADD|DATE_DIFF|EXTRACT|TO_STRING|TO_TIMESTAMP|UTCNOW|CHAR_LENGTH|CHARACTER_LENGTH|LOWER|SUBSTRING|TRIM|UPPER|LEADING|TRAILING|BOTH|FOR)\b)` +
		`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)` +
		`|(?P<QuotIdent>"([^"]*("")?)*")` +
		`|(?P<Float>\d*\.\d+([eE][-+]?\d+)?)` +
		`|(?P<Int>\d+)` +
		`|(?P<LitString>'([^']*('')?)*')` +
		`|(?P<Operators><>|!=|<=|>=|\.\*|\[\*\]|[-+*/%,.()=<>\[\]])`,
	))

	// SQLParser is used to parse SQL statements
	SQLParser = participle.MustBuild(
		&Select{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
		participle.CaseInsensitive("Timeword"),
	)
)
