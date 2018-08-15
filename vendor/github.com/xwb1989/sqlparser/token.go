/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/xwb1989/sqlparser/dependency/bytes2"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

const (
	defaultBufSize = 4096
	eofChar        = 0x100
)

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	InStream       io.Reader
	AllowComments  bool
	ForceEOF       bool
	lastChar       uint16
	Position       int
	lastToken      []byte
	LastError      error
	posVarIndex    int
	ParseTree      Statement
	partialDDL     *DDL
	nesting        int
	multi          bool
	specialComment *Tokenizer

	buf     []byte
	bufPos  int
	bufSize int
}

// NewStringTokenizer creates a new Tokenizer for the
// sql string.
func NewStringTokenizer(sql string) *Tokenizer {
	buf := []byte(sql)
	return &Tokenizer{
		buf:     buf,
		bufSize: len(buf),
	}
}

// NewTokenizer creates a new Tokenizer reading a sql
// string from the io.Reader.
func NewTokenizer(r io.Reader) *Tokenizer {
	return &Tokenizer{
		InStream: r,
		buf:      make([]byte, defaultBufSize),
	}
}

// keywords is a map of mysql keywords that fall into two categories:
// 1) keywords considered reserved by MySQL
// 2) keywords for us to handle specially in sql.y
//
// Those marked as UNUSED are likely reserved keywords. We add them here so that
// when rewriting queries we can properly backtick quote them so they don't cause issues
//
// NOTE: If you add new keywords, add them also to the reserved_keywords or
// non_reserved_keywords grammar in sql.y -- this will allow the keyword to be used
// in identifiers. See the docs for each grammar to determine which one to put it into.
var keywords = map[string]int{
	"accessible":          UNUSED,
	"add":                 ADD,
	"against":             AGAINST,
	"all":                 ALL,
	"alter":               ALTER,
	"analyze":             ANALYZE,
	"and":                 AND,
	"as":                  AS,
	"asc":                 ASC,
	"asensitive":          UNUSED,
	"auto_increment":      AUTO_INCREMENT,
	"before":              UNUSED,
	"begin":               BEGIN,
	"between":             BETWEEN,
	"bigint":              BIGINT,
	"binary":              BINARY,
	"_binary":             UNDERSCORE_BINARY,
	"bit":                 BIT,
	"blob":                BLOB,
	"bool":                BOOL,
	"boolean":             BOOLEAN,
	"both":                UNUSED,
	"by":                  BY,
	"call":                UNUSED,
	"cascade":             UNUSED,
	"case":                CASE,
	"cast":                CAST,
	"change":              UNUSED,
	"char":                CHAR,
	"character":           CHARACTER,
	"charset":             CHARSET,
	"check":               UNUSED,
	"collate":             COLLATE,
	"column":              COLUMN,
	"comment":             COMMENT_KEYWORD,
	"committed":           COMMITTED,
	"commit":              COMMIT,
	"condition":           UNUSED,
	"constraint":          CONSTRAINT,
	"continue":            UNUSED,
	"convert":             CONVERT,
	"substr":              SUBSTR,
	"substring":           SUBSTRING,
	"create":              CREATE,
	"cross":               CROSS,
	"current_date":        CURRENT_DATE,
	"current_time":        CURRENT_TIME,
	"current_timestamp":   CURRENT_TIMESTAMP,
	"current_user":        UNUSED,
	"cursor":              UNUSED,
	"database":            DATABASE,
	"databases":           DATABASES,
	"day_hour":            UNUSED,
	"day_microsecond":     UNUSED,
	"day_minute":          UNUSED,
	"day_second":          UNUSED,
	"date":                DATE,
	"datetime":            DATETIME,
	"dec":                 UNUSED,
	"decimal":             DECIMAL,
	"declare":             UNUSED,
	"default":             DEFAULT,
	"delayed":             UNUSED,
	"delete":              DELETE,
	"desc":                DESC,
	"describe":            DESCRIBE,
	"deterministic":       UNUSED,
	"distinct":            DISTINCT,
	"distinctrow":         UNUSED,
	"div":                 DIV,
	"double":              DOUBLE,
	"drop":                DROP,
	"duplicate":           DUPLICATE,
	"each":                UNUSED,
	"else":                ELSE,
	"elseif":              UNUSED,
	"enclosed":            UNUSED,
	"end":                 END,
	"enum":                ENUM,
	"escape":              ESCAPE,
	"escaped":             UNUSED,
	"exists":              EXISTS,
	"exit":                UNUSED,
	"explain":             EXPLAIN,
	"expansion":           EXPANSION,
	"extended":            EXTENDED,
	"false":               FALSE,
	"fetch":               UNUSED,
	"float":               FLOAT_TYPE,
	"float4":              UNUSED,
	"float8":              UNUSED,
	"for":                 FOR,
	"force":               FORCE,
	"foreign":             FOREIGN,
	"from":                FROM,
	"full":                FULL,
	"fulltext":            FULLTEXT,
	"generated":           UNUSED,
	"geometry":            GEOMETRY,
	"geometrycollection":  GEOMETRYCOLLECTION,
	"get":                 UNUSED,
	"global":              GLOBAL,
	"grant":               UNUSED,
	"group":               GROUP,
	"group_concat":        GROUP_CONCAT,
	"having":              HAVING,
	"high_priority":       UNUSED,
	"hour_microsecond":    UNUSED,
	"hour_minute":         UNUSED,
	"hour_second":         UNUSED,
	"if":                  IF,
	"ignore":              IGNORE,
	"in":                  IN,
	"index":               INDEX,
	"infile":              UNUSED,
	"inout":               UNUSED,
	"inner":               INNER,
	"insensitive":         UNUSED,
	"insert":              INSERT,
	"int":                 INT,
	"int1":                UNUSED,
	"int2":                UNUSED,
	"int3":                UNUSED,
	"int4":                UNUSED,
	"int8":                UNUSED,
	"integer":             INTEGER,
	"interval":            INTERVAL,
	"into":                INTO,
	"io_after_gtids":      UNUSED,
	"is":                  IS,
	"isolation":           ISOLATION,
	"iterate":             UNUSED,
	"join":                JOIN,
	"json":                JSON,
	"key":                 KEY,
	"keys":                KEYS,
	"key_block_size":      KEY_BLOCK_SIZE,
	"kill":                UNUSED,
	"language":            LANGUAGE,
	"last_insert_id":      LAST_INSERT_ID,
	"leading":             UNUSED,
	"leave":               UNUSED,
	"left":                LEFT,
	"less":                LESS,
	"level":               LEVEL,
	"like":                LIKE,
	"limit":               LIMIT,
	"linear":              UNUSED,
	"lines":               UNUSED,
	"linestring":          LINESTRING,
	"load":                UNUSED,
	"localtime":           LOCALTIME,
	"localtimestamp":      LOCALTIMESTAMP,
	"lock":                LOCK,
	"long":                UNUSED,
	"longblob":            LONGBLOB,
	"longtext":            LONGTEXT,
	"loop":                UNUSED,
	"low_priority":        UNUSED,
	"master_bind":         UNUSED,
	"match":               MATCH,
	"maxvalue":            MAXVALUE,
	"mediumblob":          MEDIUMBLOB,
	"mediumint":           MEDIUMINT,
	"mediumtext":          MEDIUMTEXT,
	"middleint":           UNUSED,
	"minute_microsecond":  UNUSED,
	"minute_second":       UNUSED,
	"mod":                 MOD,
	"mode":                MODE,
	"modifies":            UNUSED,
	"multilinestring":     MULTILINESTRING,
	"multipoint":          MULTIPOINT,
	"multipolygon":        MULTIPOLYGON,
	"names":               NAMES,
	"natural":             NATURAL,
	"nchar":               NCHAR,
	"next":                NEXT,
	"not":                 NOT,
	"no_write_to_binlog":  UNUSED,
	"null":                NULL,
	"numeric":             NUMERIC,
	"offset":              OFFSET,
	"on":                  ON,
	"only":                ONLY,
	"optimize":            OPTIMIZE,
	"optimizer_costs":     UNUSED,
	"option":              UNUSED,
	"optionally":          UNUSED,
	"or":                  OR,
	"order":               ORDER,
	"out":                 UNUSED,
	"outer":               OUTER,
	"outfile":             UNUSED,
	"partition":           PARTITION,
	"point":               POINT,
	"polygon":             POLYGON,
	"precision":           UNUSED,
	"primary":             PRIMARY,
	"processlist":         PROCESSLIST,
	"procedure":           PROCEDURE,
	"query":               QUERY,
	"range":               UNUSED,
	"read":                READ,
	"reads":               UNUSED,
	"read_write":          UNUSED,
	"real":                REAL,
	"references":          UNUSED,
	"regexp":              REGEXP,
	"release":             UNUSED,
	"rename":              RENAME,
	"reorganize":          REORGANIZE,
	"repair":              REPAIR,
	"repeat":              UNUSED,
	"repeatable":          REPEATABLE,
	"replace":             REPLACE,
	"require":             UNUSED,
	"resignal":            UNUSED,
	"restrict":            UNUSED,
	"return":              UNUSED,
	"revoke":              UNUSED,
	"right":               RIGHT,
	"rlike":               REGEXP,
	"rollback":            ROLLBACK,
	"schema":              SCHEMA,
	"schemas":             UNUSED,
	"second_microsecond":  UNUSED,
	"select":              SELECT,
	"sensitive":           UNUSED,
	"separator":           SEPARATOR,
	"serializable":        SERIALIZABLE,
	"session":             SESSION,
	"set":                 SET,
	"share":               SHARE,
	"show":                SHOW,
	"signal":              UNUSED,
	"signed":              SIGNED,
	"smallint":            SMALLINT,
	"spatial":             SPATIAL,
	"specific":            UNUSED,
	"sql":                 UNUSED,
	"sqlexception":        UNUSED,
	"sqlstate":            UNUSED,
	"sqlwarning":          UNUSED,
	"sql_big_result":      UNUSED,
	"sql_cache":           SQL_CACHE,
	"sql_calc_found_rows": UNUSED,
	"sql_no_cache":        SQL_NO_CACHE,
	"sql_small_result":    UNUSED,
	"ssl":                 UNUSED,
	"start":               START,
	"starting":            UNUSED,
	"status":              STATUS,
	"stored":              UNUSED,
	"straight_join":       STRAIGHT_JOIN,
	"stream":              STREAM,
	"table":               TABLE,
	"tables":              TABLES,
	"terminated":          UNUSED,
	"text":                TEXT,
	"than":                THAN,
	"then":                THEN,
	"time":                TIME,
	"timestamp":           TIMESTAMP,
	"tinyblob":            TINYBLOB,
	"tinyint":             TINYINT,
	"tinytext":            TINYTEXT,
	"to":                  TO,
	"trailing":            UNUSED,
	"transaction":         TRANSACTION,
	"trigger":             TRIGGER,
	"true":                TRUE,
	"truncate":            TRUNCATE,
	"uncommitted":         UNCOMMITTED,
	"undo":                UNUSED,
	"union":               UNION,
	"unique":              UNIQUE,
	"unlock":              UNUSED,
	"unsigned":            UNSIGNED,
	"update":              UPDATE,
	"usage":               UNUSED,
	"use":                 USE,
	"using":               USING,
	"utc_date":            UTC_DATE,
	"utc_time":            UTC_TIME,
	"utc_timestamp":       UTC_TIMESTAMP,
	"values":              VALUES,
	"variables":           VARIABLES,
	"varbinary":           VARBINARY,
	"varchar":             VARCHAR,
	"varcharacter":        UNUSED,
	"varying":             UNUSED,
	"virtual":             UNUSED,
	"vindex":              VINDEX,
	"vindexes":            VINDEXES,
	"view":                VIEW,
	"vitess_keyspaces":    VITESS_KEYSPACES,
	"vitess_shards":       VITESS_SHARDS,
	"vitess_tablets":      VITESS_TABLETS,
	"vschema_tables":      VSCHEMA_TABLES,
	"when":                WHEN,
	"where":               WHERE,
	"while":               UNUSED,
	"with":                WITH,
	"write":               WRITE,
	"xor":                 UNUSED,
	"year":                YEAR,
	"year_month":          UNUSED,
	"zerofill":            ZEROFILL,
}

// keywordStrings contains the reverse mapping of token to keyword strings
var keywordStrings = map[int]string{}

func init() {
	for str, id := range keywords {
		if id == UNUSED {
			continue
		}
		keywordStrings[id] = str
	}
}

// KeywordString returns the string corresponding to the given keyword
func KeywordString(id int) string {
	str, ok := keywordStrings[id]
	if !ok {
		return ""
	}
	return str
}

// Lex returns the next token form the Tokenizer.
// This function is used by go yacc.
func (tkn *Tokenizer) Lex(lval *yySymType) int {
	typ, val := tkn.Scan()
	for typ == COMMENT {
		if tkn.AllowComments {
			break
		}
		typ, val = tkn.Scan()
	}
	lval.bytes = val
	tkn.lastToken = val
	return typ
}

// Error is called by go yacc if there's a parsing error.
func (tkn *Tokenizer) Error(err string) {
	buf := &bytes2.Buffer{}
	if tkn.lastToken != nil {
		fmt.Fprintf(buf, "%s at position %v near '%s'", err, tkn.Position, tkn.lastToken)
	} else {
		fmt.Fprintf(buf, "%s at position %v", err, tkn.Position)
	}
	tkn.LastError = errors.New(buf.String())

	// Try and re-sync to the next statement
	if tkn.lastChar != ';' {
		tkn.skipStatement()
	}
}

// Scan scans the tokenizer for the next token and returns
// the token type and an optional value.
func (tkn *Tokenizer) Scan() (int, []byte) {
	if tkn.specialComment != nil {
		// Enter specialComment scan mode.
		// for scanning such kind of comment: /*! MySQL-specific code */
		specialComment := tkn.specialComment
		tok, val := specialComment.Scan()
		if tok != 0 {
			// return the specialComment scan result as the result
			return tok, val
		}
		// leave specialComment scan mode after all stream consumed.
		tkn.specialComment = nil
	}
	if tkn.lastChar == 0 {
		tkn.next()
	}

	if tkn.ForceEOF {
		tkn.skipStatement()
		return 0, nil
	}

	tkn.skipBlank()
	switch ch := tkn.lastChar; {
	case isLetter(ch):
		tkn.next()
		if ch == 'X' || ch == 'x' {
			if tkn.lastChar == '\'' {
				tkn.next()
				return tkn.scanHex()
			}
		}
		if ch == 'B' || ch == 'b' {
			if tkn.lastChar == '\'' {
				tkn.next()
				return tkn.scanBitLiteral()
			}
		}
		isDbSystemVariable := false
		if ch == '@' && tkn.lastChar == '@' {
			isDbSystemVariable = true
		}
		return tkn.scanIdentifier(byte(ch), isDbSystemVariable)
	case isDigit(ch):
		return tkn.scanNumber(false)
	case ch == ':':
		return tkn.scanBindVar()
	case ch == ';' && tkn.multi:
		return 0, nil
	default:
		tkn.next()
		switch ch {
		case eofChar:
			return 0, nil
		case '=', ',', ';', '(', ')', '+', '*', '%', '^', '~':
			return int(ch), nil
		case '&':
			if tkn.lastChar == '&' {
				tkn.next()
				return AND, nil
			}
			return int(ch), nil
		case '|':
			if tkn.lastChar == '|' {
				tkn.next()
				return OR, nil
			}
			return int(ch), nil
		case '?':
			tkn.posVarIndex++
			buf := new(bytes2.Buffer)
			fmt.Fprintf(buf, ":v%d", tkn.posVarIndex)
			return VALUE_ARG, buf.Bytes()
		case '.':
			if isDigit(tkn.lastChar) {
				return tkn.scanNumber(true)
			}
			return int(ch), nil
		case '/':
			switch tkn.lastChar {
			case '/':
				tkn.next()
				return tkn.scanCommentType1("//")
			case '*':
				tkn.next()
				switch tkn.lastChar {
				case '!':
					return tkn.scanMySQLSpecificComment()
				default:
					return tkn.scanCommentType2()
				}
			default:
				return int(ch), nil
			}
		case '#':
			return tkn.scanCommentType1("#")
		case '-':
			switch tkn.lastChar {
			case '-':
				tkn.next()
				return tkn.scanCommentType1("--")
			case '>':
				tkn.next()
				if tkn.lastChar == '>' {
					tkn.next()
					return JSON_UNQUOTE_EXTRACT_OP, nil
				}
				return JSON_EXTRACT_OP, nil
			}
			return int(ch), nil
		case '<':
			switch tkn.lastChar {
			case '>':
				tkn.next()
				return NE, nil
			case '<':
				tkn.next()
				return SHIFT_LEFT, nil
			case '=':
				tkn.next()
				switch tkn.lastChar {
				case '>':
					tkn.next()
					return NULL_SAFE_EQUAL, nil
				default:
					return LE, nil
				}
			default:
				return int(ch), nil
			}
		case '>':
			switch tkn.lastChar {
			case '=':
				tkn.next()
				return GE, nil
			case '>':
				tkn.next()
				return SHIFT_RIGHT, nil
			default:
				return int(ch), nil
			}
		case '!':
			if tkn.lastChar == '=' {
				tkn.next()
				return NE, nil
			}
			return int(ch), nil
		case '\'', '"':
			return tkn.scanString(ch, STRING)
		case '`':
			return tkn.scanLiteralIdentifier()
		default:
			return LEX_ERROR, []byte{byte(ch)}
		}
	}
}

// skipStatement scans until the EOF, or end of statement is encountered.
func (tkn *Tokenizer) skipStatement() {
	ch := tkn.lastChar
	for ch != ';' && ch != eofChar {
		tkn.next()
		ch = tkn.lastChar
	}
}

func (tkn *Tokenizer) skipBlank() {
	ch := tkn.lastChar
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		tkn.next()
		ch = tkn.lastChar
	}
}

func (tkn *Tokenizer) scanIdentifier(firstByte byte, isDbSystemVariable bool) (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteByte(firstByte)
	for isLetter(tkn.lastChar) || isDigit(tkn.lastChar) || (isDbSystemVariable && isCarat(tkn.lastChar)) {
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	lowered := bytes.ToLower(buffer.Bytes())
	loweredStr := string(lowered)
	if keywordID, found := keywords[loweredStr]; found {
		return keywordID, lowered
	}
	// dual must always be case-insensitive
	if loweredStr == "dual" {
		return ID, lowered
	}
	return ID, buffer.Bytes()
}

func (tkn *Tokenizer) scanHex() (int, []byte) {
	buffer := &bytes2.Buffer{}
	tkn.scanMantissa(16, buffer)
	if tkn.lastChar != '\'' {
		return LEX_ERROR, buffer.Bytes()
	}
	tkn.next()
	if buffer.Len()%2 != 0 {
		return LEX_ERROR, buffer.Bytes()
	}
	return HEX, buffer.Bytes()
}

func (tkn *Tokenizer) scanBitLiteral() (int, []byte) {
	buffer := &bytes2.Buffer{}
	tkn.scanMantissa(2, buffer)
	if tkn.lastChar != '\'' {
		return LEX_ERROR, buffer.Bytes()
	}
	tkn.next()
	return BIT_LITERAL, buffer.Bytes()
}

func (tkn *Tokenizer) scanLiteralIdentifier() (int, []byte) {
	buffer := &bytes2.Buffer{}
	backTickSeen := false
	for {
		if backTickSeen {
			if tkn.lastChar != '`' {
				break
			}
			backTickSeen = false
			buffer.WriteByte('`')
			tkn.next()
			continue
		}
		// The previous char was not a backtick.
		switch tkn.lastChar {
		case '`':
			backTickSeen = true
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, buffer.Bytes()
		default:
			buffer.WriteByte(byte(tkn.lastChar))
		}
		tkn.next()
	}
	if buffer.Len() == 0 {
		return LEX_ERROR, buffer.Bytes()
	}
	return ID, buffer.Bytes()
}

func (tkn *Tokenizer) scanBindVar() (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteByte(byte(tkn.lastChar))
	token := VALUE_ARG
	tkn.next()
	if tkn.lastChar == ':' {
		token = LIST_ARG
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	if !isLetter(tkn.lastChar) {
		return LEX_ERROR, buffer.Bytes()
	}
	for isLetter(tkn.lastChar) || isDigit(tkn.lastChar) || tkn.lastChar == '.' {
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	return token, buffer.Bytes()
}

func (tkn *Tokenizer) scanMantissa(base int, buffer *bytes2.Buffer) {
	for digitVal(tkn.lastChar) < base {
		tkn.consumeNext(buffer)
	}
}

func (tkn *Tokenizer) scanNumber(seenDecimalPoint bool) (int, []byte) {
	token := INTEGRAL
	buffer := &bytes2.Buffer{}
	if seenDecimalPoint {
		token = FLOAT
		buffer.WriteByte('.')
		tkn.scanMantissa(10, buffer)
		goto exponent
	}

	// 0x construct.
	if tkn.lastChar == '0' {
		tkn.consumeNext(buffer)
		if tkn.lastChar == 'x' || tkn.lastChar == 'X' {
			token = HEXNUM
			tkn.consumeNext(buffer)
			tkn.scanMantissa(16, buffer)
			goto exit
		}
	}

	tkn.scanMantissa(10, buffer)

	if tkn.lastChar == '.' {
		token = FLOAT
		tkn.consumeNext(buffer)
		tkn.scanMantissa(10, buffer)
	}

exponent:
	if tkn.lastChar == 'e' || tkn.lastChar == 'E' {
		token = FLOAT
		tkn.consumeNext(buffer)
		if tkn.lastChar == '+' || tkn.lastChar == '-' {
			tkn.consumeNext(buffer)
		}
		tkn.scanMantissa(10, buffer)
	}

exit:
	// A letter cannot immediately follow a number.
	if isLetter(tkn.lastChar) {
		return LEX_ERROR, buffer.Bytes()
	}

	return token, buffer.Bytes()
}

func (tkn *Tokenizer) scanString(delim uint16, typ int) (int, []byte) {
	var buffer bytes2.Buffer
	for {
		ch := tkn.lastChar
		if ch == eofChar {
			// Unterminated string.
			return LEX_ERROR, buffer.Bytes()
		}

		if ch != delim && ch != '\\' {
			buffer.WriteByte(byte(ch))

			// Scan ahead to the next interesting character.
			start := tkn.bufPos
			for ; tkn.bufPos < tkn.bufSize; tkn.bufPos++ {
				ch = uint16(tkn.buf[tkn.bufPos])
				if ch == delim || ch == '\\' {
					break
				}
			}

			buffer.Write(tkn.buf[start:tkn.bufPos])
			tkn.Position += (tkn.bufPos - start)

			if tkn.bufPos >= tkn.bufSize {
				// Reached the end of the buffer without finding a delim or
				// escape character.
				tkn.next()
				continue
			}

			tkn.bufPos++
			tkn.Position++
		}
		tkn.next() // Read one past the delim or escape character.

		if ch == '\\' {
			if tkn.lastChar == eofChar {
				// String terminates mid escape character.
				return LEX_ERROR, buffer.Bytes()
			}
			if decodedChar := sqltypes.SQLDecodeMap[byte(tkn.lastChar)]; decodedChar == sqltypes.DontEscape {
				ch = tkn.lastChar
			} else {
				ch = uint16(decodedChar)
			}

		} else if ch == delim && tkn.lastChar != delim {
			// Correctly terminated string, which is not a double delim.
			break
		}

		buffer.WriteByte(byte(ch))
		tkn.next()
	}

	return typ, buffer.Bytes()
}

func (tkn *Tokenizer) scanCommentType1(prefix string) (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteString(prefix)
	for tkn.lastChar != eofChar {
		if tkn.lastChar == '\n' {
			tkn.consumeNext(buffer)
			break
		}
		tkn.consumeNext(buffer)
	}
	return COMMENT, buffer.Bytes()
}

func (tkn *Tokenizer) scanCommentType2() (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteString("/*")
	for {
		if tkn.lastChar == '*' {
			tkn.consumeNext(buffer)
			if tkn.lastChar == '/' {
				tkn.consumeNext(buffer)
				break
			}
			continue
		}
		if tkn.lastChar == eofChar {
			return LEX_ERROR, buffer.Bytes()
		}
		tkn.consumeNext(buffer)
	}
	return COMMENT, buffer.Bytes()
}

func (tkn *Tokenizer) scanMySQLSpecificComment() (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteString("/*!")
	tkn.next()
	for {
		if tkn.lastChar == '*' {
			tkn.consumeNext(buffer)
			if tkn.lastChar == '/' {
				tkn.consumeNext(buffer)
				break
			}
			continue
		}
		if tkn.lastChar == eofChar {
			return LEX_ERROR, buffer.Bytes()
		}
		tkn.consumeNext(buffer)
	}
	_, sql := ExtractMysqlComment(buffer.String())
	tkn.specialComment = NewStringTokenizer(sql)
	return tkn.Scan()
}

func (tkn *Tokenizer) consumeNext(buffer *bytes2.Buffer) {
	if tkn.lastChar == eofChar {
		// This should never happen.
		panic("unexpected EOF")
	}
	buffer.WriteByte(byte(tkn.lastChar))
	tkn.next()
}

func (tkn *Tokenizer) next() {
	if tkn.bufPos >= tkn.bufSize && tkn.InStream != nil {
		// Try and refill the buffer
		var err error
		tkn.bufPos = 0
		if tkn.bufSize, err = tkn.InStream.Read(tkn.buf); err != io.EOF && err != nil {
			tkn.LastError = err
		}
	}

	if tkn.bufPos >= tkn.bufSize {
		if tkn.lastChar != eofChar {
			tkn.Position++
			tkn.lastChar = eofChar
		}
	} else {
		tkn.Position++
		tkn.lastChar = uint16(tkn.buf[tkn.bufPos])
		tkn.bufPos++
	}
}

// reset clears any internal state.
func (tkn *Tokenizer) reset() {
	tkn.ParseTree = nil
	tkn.partialDDL = nil
	tkn.specialComment = nil
	tkn.posVarIndex = 0
	tkn.nesting = 0
	tkn.ForceEOF = false
}

func isLetter(ch uint16) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '@'
}

func isCarat(ch uint16) bool {
	return ch == '.' || ch == '\'' || ch == '"' || ch == '`'
}

func digitVal(ch uint16) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch) - '0'
	case 'a' <= ch && ch <= 'f':
		return int(ch) - 'a' + 10
	case 'A' <= ch && ch <= 'F':
		return int(ch) - 'A' + 10
	}
	return 16 // larger than any legal digit val
}

func isDigit(ch uint16) bool {
	return '0' <= ch && ch <= '9'
}
