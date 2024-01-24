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
	"bytes"
	"testing"

	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
)

func TestJSONPathElement(t *testing.T) {
	p := participle.MustBuild(
		&JSONPathElement{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
		participle.CaseInsensitive("Timeword"),
	)

	j := JSONPathElement{}
	cases := []string{
		// Key
		"['name']", ".name", `."name"`,

		// Index
		"[2]", "[0]", "[100]",

		// Object wildcard
		".*",

		// array wildcard
		"[*]",
	}
	for i, tc := range cases {
		err := p.ParseString(tc, &j)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		// repr.Println(j, repr.Indent("  "), repr.OmitEmpty(true))
	}
}

func TestJSONPath(t *testing.T) {
	p := participle.MustBuild(
		&JSONPath{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
		participle.CaseInsensitive("Timeword"),
	)

	j := JSONPath{}
	cases := []string{
		"S3Object",
		"S3Object.id",
		"S3Object.book.title",
		"S3Object.id[1]",
		"S3Object.id['abc']",
		"S3Object.id['ab']",
		"S3Object.words.*.id",
		"S3Object.words.name[*].val",
		"S3Object.words.name[*].val[*]",
		"S3Object.words.name[*].val.*",
	}
	for i, tc := range cases {
		err := p.ParseString(tc, &j)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		// repr.Println(j, repr.Indent("  "), repr.OmitEmpty(true))
	}
}

func TestIdentifierParsing(t *testing.T) {
	p := participle.MustBuild(
		&Identifier{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
	)

	id := Identifier{}
	validCases := []string{
		"a",
		"_a",
		"abc_a",
		"a2",
		`"abc"`,
		`"abc\a""ac"`,
	}
	for i, tc := range validCases {
		err := p.ParseString(tc, &id)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		// repr.Println(id, repr.Indent("  "), repr.OmitEmpty(true))
	}

	invalidCases := []string{
		"+a",
		"-a",
		"1a",
		`"ab`,
		`abc"`,
		`aa""a`,
		`"a"a"`,
	}
	for i, tc := range invalidCases {
		err := p.ParseString(tc, &id)
		if err == nil {
			t.Fatalf("%d: %v", i, err)
		}
		// fmt.Println(tc, err)
	}
}

func TestLiteralStringParsing(t *testing.T) {
	var k ObjectKey
	p := participle.MustBuild(
		&ObjectKey{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
	)

	validCases := []string{
		"['abc']",
		"['ab''c']",
		"['a''b''c']",
		"['abc-x_1##@(*&(#*))/\\']",
	}
	for i, tc := range validCases {
		err := p.ParseString(tc, &k)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if string(*k.Lit) == "" {
			t.Fatalf("Incorrect parse %#v", k)
		}
		// repr.Println(k, repr.Indent("  "), repr.OmitEmpty(true))
	}

	invalidCases := []string{
		"['abc'']",
		"['-abc'sc']",
		"[abc']",
		"['ac]",
	}
	for i, tc := range invalidCases {
		err := p.ParseString(tc, &k)
		if err == nil {
			t.Fatalf("%d: %v", i, err)
		}
		// fmt.Println(tc, err)
	}
}

func TestFunctionParsing(t *testing.T) {
	var fex FuncExpr
	p := participle.MustBuild(
		&FuncExpr{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
		participle.CaseInsensitive("Timeword"),
	)

	validCases := []string{
		"count(*)",
		"sum(2 + s.id)",
		"sum(t)",
		"avg(s.id[1])",
		"coalesce(s.id[1], 2, 2 + 3)",

		"cast(s as string)",
		"cast(s AS INT)",
		"cast(s as DECIMAL)",
		"extract(YEAR from '2018-01-09')",
		"extract(month from '2018-01-09')",

		"extract(hour from '2018-01-09')",
		"extract(day from '2018-01-09')",
		"substring('abcd' from 2 for 2)",
		"substring('abcd' from 2)",
		"substring('abcd' , 2 , 2)",

		"substring('abcd' , 22 )",
		"trim('  aab  ')",
		"trim(leading from '  aab  ')",
		"trim(trailing from '  aab  ')",
		"trim(both from '  aab  ')",

		"trim(both '12' from '  aab  ')",
		"trim(leading '12' from '  aab  ')",
		"trim(trailing '12' from '  aab  ')",
		"count(23)",
	}
	for i, tc := range validCases {
		err := p.ParseString(tc, &fex)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		// repr.Println(fex, repr.Indent("  "), repr.OmitEmpty(true))
	}
}

func TestSqlLexer(t *testing.T) {
	// s := bytes.NewBuffer([]byte("s.['name'].*.[*].abc.[\"abc\"]"))
	s := bytes.NewBuffer([]byte("S3Object.words.*.id"))
	// s := bytes.NewBuffer([]byte("COUNT(Id)"))
	lex, err := sqlLexer.Lex(s)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := lexer.ConsumeAll(lex)
	if err != nil {
		t.Fatal(err)
	}
	// for i, t := range tokens {
	// 	fmt.Printf("%d: %#v\n", i, t)
	// }
	if len(tokens) != 7 {
		t.Fatalf("Expected 7 got %d", len(tokens))
	}
}

func TestSelectWhere(t *testing.T) {
	p := participle.MustBuild(
		&Select{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
	)

	s := Select{}
	cases := []string{
		"select * from s3object",
		"select a, b from s3object s",
		"select a, b from s3object as s",
		"select a, b from s3object as s where a = 1",
		"select a, b from s3object s where a = 1",
		"select a, b from s3object where a = 1",
	}
	for i, tc := range cases {
		err := p.ParseString(tc, &s)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		// repr.Println(s, repr.Indent("  "), repr.OmitEmpty(true))
	}
}

func TestLikeClause(t *testing.T) {
	p := participle.MustBuild(
		&Select{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
	)

	s := Select{}
	cases := []string{
		`select * from s3object where Name like 'abcd'`,
		`select Name like 'abc' from s3object`,
		`select * from s3object where Name not like 'abc'`,
		`select * from s3object where Name like 'abc' escape 't'`,
		`select * from s3object where Name like 'a\%' escape '?'`,
		`select * from s3object where Name not like 'abc\' escape '?'`,
		`select * from s3object where Name like 'a\%' escape LOWER('?')`,
		`select * from s3object where Name not like LOWER('Bc\') escape '?'`,
	}
	for i, tc := range cases {
		err := p.ParseString(tc, &s)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}
}

func TestBetweenClause(t *testing.T) {
	p := participle.MustBuild(
		&Select{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
	)

	s := Select{}
	cases := []string{
		`select * from s3object where Id between 1 and 2`,
		`select * from s3object where Id between 1 and 2 and name = 'Ab'`,
		`select * from s3object where Id not between 1 and 2`,
		`select * from s3object where Id not between 1 and 2 and name = 'Bc'`,
	}
	for i, tc := range cases {
		err := p.ParseString(tc, &s)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}
}

func TestFromClauseJSONPath(t *testing.T) {
	p := participle.MustBuild(
		&Select{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
	)

	s := Select{}
	cases := []string{
		"select * from s3object",
		"select * from s3object[*].name",
		"select * from s3object[*].books[*]",
		"select * from s3object[*].books[*].name",
		"select * from s3object where name > 2",
		"select * from s3object[*].name where name > 2",
		"select * from s3object[*].books[*] where name > 2",
		"select * from s3object[*].books[*].name where name > 2",
		"select * from s3object[*].books[*] s",
		"select * from s3object[*].books[*].name as s",
		"select * from s3object s where name > 2",
		"select * from s3object[*].name as s where name > 2",
		"select * from s3object[*].books[*] limit 1",
	}
	for i, tc := range cases {
		err := p.ParseString(tc, &s)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		// repr.Println(s, repr.Indent("  "), repr.OmitEmpty(true))
	}
}

func TestSelectParsing(t *testing.T) {
	p := participle.MustBuild(
		&Select{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
	)

	s := Select{}
	cases := []string{
		"select * from s3object where name > 2 or value > 1 or word > 2",
		"select s.word.id + 2 from s3object s",
		"select 1-2-3 from s3object s limit 1",
	}
	for i, tc := range cases {
		err := p.ParseString(tc, &s)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		// repr.Println(s, repr.Indent("  "), repr.OmitEmpty(true))
	}
}

func TestSqlLexerArithOps(t *testing.T) {
	s := bytes.NewBuffer([]byte("year from select month hour distinct"))
	lex, err := sqlLexer.Lex(s)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := lexer.ConsumeAll(lex)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 7 {
		t.Errorf("Expected 7 got %d", len(tokens))
	}
	// for i, t := range tokens {
	// 	fmt.Printf("%d: %#v\n", i, t)
	// }
}

func TestParseSelectStatement(t *testing.T) {
	exp, err := ParseSelectStatement("select _3,_1,_2 as 'mytest'  from S3object")
	if err != nil {
		t.Fatalf("parse alias sql error: %v", err)
	}
	if exp.selectAST.Expression.Expressions[2].As != "mytest" {
		t.Fatalf("parse alias sql error: %s not equal %s", exp.selectAST.Expression.Expressions[2].As, err)
	}
}
