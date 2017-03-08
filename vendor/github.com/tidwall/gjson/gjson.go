// Package gjson provides searching for json strings.
package gjson

import (
	"reflect"
	"strconv"

	// It's totally safe to use this package, but in case your
	// project or organization restricts the use of 'unsafe',
	// there's the "github.com/tidwall/gjson-safe" package.
	"unsafe"

	"github.com/tidwall/match"
)

// Type is Result type
type Type int

const (
	// Null is a null json value
	Null Type = iota
	// False is a json false boolean
	False
	// Number is json number
	Number
	// String is a json string
	String
	// True is a json true boolean
	True
	// JSON is a raw block of JSON
	JSON
)

// String returns a string representation of the type.
func (t Type) String() string {
	switch t {
	default:
		return ""
	case Null:
		return "Null"
	case False:
		return "False"
	case Number:
		return "Number"
	case String:
		return "String"
	case True:
		return "True"
	case JSON:
		return "JSON"
	}
}

// Result represents a json value that is returned from Get().
type Result struct {
	// Type is the json type
	Type Type
	// Raw is the raw json
	Raw string
	// Str is the json string
	Str string
	// Num is the json number
	Num float64
	// Index of raw value in original json, zero means index unknown
	Index int
}

// String returns a string representation of the value.
func (t Result) String() string {
	switch t.Type {
	default:
		return "null"
	case False:
		return "false"
	case Number:
		return strconv.FormatFloat(t.Num, 'f', -1, 64)
	case String:
		return t.Str
	case JSON:
		return t.Raw
	case True:
		return "true"
	}
}

// Bool returns an boolean representation.
func (t Result) Bool() bool {
	switch t.Type {
	default:
		return false
	case True:
		return true
	case String:
		return t.Str != "" && t.Str != "0"
	case Number:
		return t.Num != 0
	}
}

// Int returns an integer representation.
func (t Result) Int() int64 {
	switch t.Type {
	default:
		return 0
	case True:
		return 1
	case String:
		n, _ := strconv.ParseInt(t.Str, 10, 64)
		return n
	case Number:
		return int64(t.Num)
	}
}

// Uint returns an unsigned integer representation.
func (t Result) Uint() uint64 {
	switch t.Type {
	default:
		return 0
	case True:
		return 1
	case String:
		n, _ := strconv.ParseUint(t.Str, 10, 64)
		return n
	case Number:
		return uint64(t.Num)
	}
}

// Float returns an float64 representation.
func (t Result) Float() float64 {
	switch t.Type {
	default:
		return 0
	case True:
		return 1
	case String:
		n, _ := strconv.ParseFloat(t.Str, 64)
		return n
	case Number:
		return t.Num
	}
}

// Array returns back an array of values.
// If the result represents a non-existent value, then an empty array will be returned.
// If the result is not a JSON array, the return value will be an array containing one result.
func (t Result) Array() []Result {
	if !t.Exists() {
		return nil
	}
	if t.Type != JSON {
		return []Result{t}
	}
	r := t.arrayOrMap('[', false)
	return r.a
}

// ForEach iterates through values.
// If the result represents a non-existent value, then no values will be iterated.
// If the result is an Object, the iterator will pass the key and value of each item.
// If the result is an Array, the iterator will only pass the value of each item.
// If the result is not a JSON array or object, the iterator will pass back one value equal to the result.
func (t Result) ForEach(iterator func(key, value Result) bool) {
	if !t.Exists() {
		return
	}
	if t.Type != JSON {
		iterator(Result{}, t)
		return
	}
	json := t.Raw
	var keys bool
	var i int
	var key, value Result
	for ; i < len(json); i++ {
		if json[i] == '{' {
			i++
			key.Type = String
			keys = true
			break
		} else if json[i] == '[' {
			i++
			break
		}
		if json[i] > ' ' {
			return
		}
	}
	var str string
	var vesc bool
	var ok bool
	for ; i < len(json); i++ {
		if keys {
			if json[i] != '"' {
				continue
			}
			s := i
			i, str, vesc, ok = parseString(json, i+1)
			if !ok {
				return
			}
			if vesc {
				key.Str = unescape(str[1 : len(str)-1])
			} else {
				key.Str = str[1 : len(str)-1]
			}
			key.Raw = str
			key.Index = s
		}
		for ; i < len(json); i++ {
			if json[i] <= ' ' || json[i] == ',' || json[i] == ':' {
				continue
			}
			break
		}
		s := i
		i, value, ok = parseAny(json, i, true)
		if !ok {
			return
		}
		value.Index = s
		if !iterator(key, value) {
			return
		}
	}
}

// Map returns back an map of values. The result should be a JSON array.
func (t Result) Map() map[string]Result {
	if t.Type != JSON {
		return map[string]Result{}
	}
	r := t.arrayOrMap('{', false)
	return r.o
}

// Get searches result for the specified path.
// The result should be a JSON array or object.
func (t Result) Get(path string) Result {
	return Get(t.Raw, path)
}

type arrayOrMapResult struct {
	a  []Result
	ai []interface{}
	o  map[string]Result
	oi map[string]interface{}
	vc byte
}

func (t Result) arrayOrMap(vc byte, valueize bool) (r arrayOrMapResult) {
	var json = t.Raw
	var i int
	var value Result
	var count int
	var key Result
	if vc == 0 {
		for ; i < len(json); i++ {
			if json[i] == '{' || json[i] == '[' {
				r.vc = json[i]
				i++
				break
			}
			if json[i] > ' ' {
				goto end
			}
		}
	} else {
		for ; i < len(json); i++ {
			if json[i] == vc {
				i++
				break
			}
			if json[i] > ' ' {
				goto end
			}
		}
		r.vc = vc
	}
	if r.vc == '{' {
		if valueize {
			r.oi = make(map[string]interface{})
		} else {
			r.o = make(map[string]Result)
		}
	} else {
		if valueize {
			r.ai = make([]interface{}, 0)
		} else {
			r.a = make([]Result, 0)
		}
	}
	for ; i < len(json); i++ {
		if json[i] <= ' ' {
			continue
		}
		// get next value
		if json[i] == ']' || json[i] == '}' {
			break
		}
		switch json[i] {
		default:
			if (json[i] >= '0' && json[i] <= '9') || json[i] == '-' {
				value.Type = Number
				value.Raw, value.Num = tonum(json[i:])
			} else {
				continue
			}
		case '{', '[':
			value.Type = JSON
			value.Raw = squash(json[i:])
		case 'n':
			value.Type = Null
			value.Raw = tolit(json[i:])
		case 't':
			value.Type = True
			value.Raw = tolit(json[i:])
		case 'f':
			value.Type = False
			value.Raw = tolit(json[i:])
		case '"':
			value.Type = String
			value.Raw, value.Str = tostr(json[i:])
		}
		i += len(value.Raw) - 1

		if r.vc == '{' {
			if count%2 == 0 {
				key = value
			} else {
				if valueize {
					r.oi[key.Str] = value.Value()
				} else {
					r.o[key.Str] = value
				}
			}
			count++
		} else {
			if valueize {
				r.ai = append(r.ai, value.Value())
			} else {
				r.a = append(r.a, value)
			}
		}
	}
end:
	return
}

// Parse parses the json and returns a result.
func Parse(json string) Result {
	var value Result
	for i := 0; i < len(json); i++ {
		if json[i] == '{' || json[i] == '[' {
			value.Type = JSON
			value.Raw = json[i:] // just take the entire raw
			break
		}
		if json[i] <= ' ' {
			continue
		}
		switch json[i] {
		default:
			if (json[i] >= '0' && json[i] <= '9') || json[i] == '-' {
				value.Type = Number
				value.Raw, value.Num = tonum(json[i:])
			} else {
				return Result{}
			}
		case 'n':
			value.Type = Null
			value.Raw = tolit(json[i:])
		case 't':
			value.Type = True
			value.Raw = tolit(json[i:])
		case 'f':
			value.Type = False
			value.Raw = tolit(json[i:])
		case '"':
			value.Type = String
			value.Raw, value.Str = tostr(json[i:])
		}
		break
	}
	return value
}

// ParseBytes parses the json and returns a result.
// If working with bytes, this method preferred over Parse(string(data))
func ParseBytes(json []byte) Result {
	return Parse(string(json))
}

func squash(json string) string {
	// expects that the lead character is a '[' or '{'
	// squash the value, ignoring all nested arrays and objects.
	// the first '[' or '{' has already been read
	depth := 1
	for i := 1; i < len(json); i++ {
		if json[i] >= '"' && json[i] <= '}' {
			switch json[i] {
			case '"':
				i++
				s2 := i
				for ; i < len(json); i++ {
					if json[i] > '\\' {
						continue
					}
					if json[i] == '"' {
						// look for an escaped slash
						if json[i-1] == '\\' {
							n := 0
							for j := i - 2; j > s2-1; j-- {
								if json[j] != '\\' {
									break
								}
								n++
							}
							if n%2 == 0 {
								continue
							}
						}
						break
					}
				}
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return json[:i+1]
				}
			}
		}
	}
	return json
}

func tonum(json string) (raw string, num float64) {
	for i := 1; i < len(json); i++ {
		// less than dash might have valid characters
		if json[i] <= '-' {
			if json[i] <= ' ' || json[i] == ',' {
				// break on whitespace and comma
				raw = json[:i]
				num, _ = strconv.ParseFloat(raw, 64)
				return
			}
			// could be a '+' or '-'. let's assume so.
			continue
		}
		if json[i] < ']' {
			// probably a valid number
			continue
		}
		if json[i] == 'e' || json[i] == 'E' {
			// allow for exponential numbers
			continue
		}
		// likely a ']' or '}'
		raw = json[:i]
		num, _ = strconv.ParseFloat(raw, 64)
		return
	}
	raw = json
	num, _ = strconv.ParseFloat(raw, 64)
	return
}

func tolit(json string) (raw string) {
	for i := 1; i < len(json); i++ {
		if json[i] <= 'a' || json[i] >= 'z' {
			return json[:i]
		}
	}
	return json
}

func tostr(json string) (raw string, str string) {
	// expects that the lead character is a '"'
	for i := 1; i < len(json); i++ {
		if json[i] > '\\' {
			continue
		}
		if json[i] == '"' {
			return json[:i+1], json[1:i]
		}
		if json[i] == '\\' {
			i++
			for ; i < len(json); i++ {
				if json[i] > '\\' {
					continue
				}
				if json[i] == '"' {
					// look for an escaped slash
					if json[i-1] == '\\' {
						n := 0
						for j := i - 2; j > 0; j-- {
							if json[j] != '\\' {
								break
							}
							n++
						}
						if n%2 == 0 {
							continue
						}
					}
					break
				}
			}
			var ret string
			if i+1 < len(json) {
				ret = json[:i+1]
			} else {
				ret = json[:i]
			}
			return ret, unescape(json[1:i])
		}
	}
	return json, json[1:]
}

// Exists returns true if value exists.
//
//  if gjson.Get(json, "name.last").Exists(){
//		println("value exists")
//  }
func (t Result) Exists() bool {
	return t.Type != Null || len(t.Raw) != 0
}

// Value returns one of these types:
//
//	bool, for JSON booleans
//	float64, for JSON numbers
//	Number, for JSON numbers
//	string, for JSON string literals
//	nil, for JSON null
//
func (t Result) Value() interface{} {
	if t.Type == String {
		return t.Str
	}
	switch t.Type {
	default:
		return nil
	case False:
		return false
	case Number:
		return t.Num
	case JSON:
		r := t.arrayOrMap(0, true)
		if r.vc == '{' {
			return r.oi
		} else if r.vc == '[' {
			return r.ai
		}
		return nil
	case True:
		return true
	}
}

func parseString(json string, i int) (int, string, bool, bool) {
	var s = i
	for ; i < len(json); i++ {
		if json[i] > '\\' {
			continue
		}
		if json[i] == '"' {
			return i + 1, json[s-1 : i+1], false, true
		}
		if json[i] == '\\' {
			i++
			for ; i < len(json); i++ {
				if json[i] > '\\' {
					continue
				}
				if json[i] == '"' {
					// look for an escaped slash
					if json[i-1] == '\\' {
						n := 0
						for j := i - 2; j > 0; j-- {
							if json[j] != '\\' {
								break
							}
							n++
						}
						if n%2 == 0 {
							continue
						}
					}
					return i + 1, json[s-1 : i+1], true, true
				}
			}
			break
		}
	}
	return i, json[s-1:], false, false
}

func parseNumber(json string, i int) (int, string) {
	var s = i
	i++
	for ; i < len(json); i++ {
		if json[i] <= ' ' || json[i] == ',' || json[i] == ']' || json[i] == '}' {
			return i, json[s:i]
		}
	}
	return i, json[s:]
}

func parseLiteral(json string, i int) (int, string) {
	var s = i
	i++
	for ; i < len(json); i++ {
		if json[i] < 'a' || json[i] > 'z' {
			return i, json[s:i]
		}
	}
	return i, json[s:]
}

type arrayPathResult struct {
	part    string
	path    string
	more    bool
	alogok  bool
	arrch   bool
	alogkey string
	query   struct {
		on    bool
		path  string
		op    string
		value string
		all   bool
	}
}

func parseArrayPath(path string) (r arrayPathResult) {
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			r.part = path[:i]
			r.path = path[i+1:]
			r.more = true
			return
		}
		if path[i] == '#' {
			r.arrch = true
			if i == 0 && len(path) > 1 {
				if path[1] == '.' {
					r.alogok = true
					r.alogkey = path[2:]
					r.path = path[:1]
				} else if path[1] == '[' {
					r.query.on = true
					// query
					i += 2
					// whitespace
					for ; i < len(path); i++ {
						if path[i] > ' ' {
							break
						}
					}
					s := i
					for ; i < len(path); i++ {
						if path[i] <= ' ' ||
							path[i] == '!' ||
							path[i] == '=' ||
							path[i] == '<' ||
							path[i] == '>' ||
							path[i] == '%' ||
							path[i] == ']' {
							break
						}
					}
					r.query.path = path[s:i]
					// whitespace
					for ; i < len(path); i++ {
						if path[i] > ' ' {
							break
						}
					}
					if i < len(path) {
						s = i
						if path[i] == '!' {
							if i < len(path)-1 && path[i+1] == '=' {
								i++
							}
						} else if path[i] == '<' || path[i] == '>' {
							if i < len(path)-1 && path[i+1] == '=' {
								i++
							}
						} else if path[i] == '=' {
							if i < len(path)-1 && path[i+1] == '=' {
								s++
								i++
							}
						}
						i++
						r.query.op = path[s:i]
						// whitespace
						for ; i < len(path); i++ {
							if path[i] > ' ' {
								break
							}
						}
						s = i
						for ; i < len(path); i++ {
							if path[i] == '"' {
								i++
								s2 := i
								for ; i < len(path); i++ {
									if path[i] > '\\' {
										continue
									}
									if path[i] == '"' {
										// look for an escaped slash
										if path[i-1] == '\\' {
											n := 0
											for j := i - 2; j > s2-1; j-- {
												if path[j] != '\\' {
													break
												}
												n++
											}
											if n%2 == 0 {
												continue
											}
										}
										break
									}
								}
							} else if path[i] == ']' {
								if i+1 < len(path) && path[i+1] == '#' {
									r.query.all = true
								}
								break
							}
						}
						if i > len(path) {
							i = len(path)
						}
						v := path[s:i]
						for len(v) > 0 && v[len(v)-1] <= ' ' {
							v = v[:len(v)-1]
						}
						r.query.value = v
					}
				}
			}
			continue
		}
	}
	r.part = path
	r.path = ""
	return
}

type objectPathResult struct {
	part string
	path string
	wild bool
	more bool
}

func parseObjectPath(path string) (r objectPathResult) {
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			r.part = path[:i]
			r.path = path[i+1:]
			r.more = true
			return
		}
		if path[i] == '*' || path[i] == '?' {
			r.wild = true
			continue
		}
		if path[i] == '\\' {
			// go into escape mode. this is a slower path that
			// strips off the escape character from the part.
			epart := []byte(path[:i])
			i++
			if i < len(path) {
				epart = append(epart, path[i])
				i++
				for ; i < len(path); i++ {
					if path[i] == '\\' {
						i++
						if i < len(path) {
							epart = append(epart, path[i])
						}
						continue
					} else if path[i] == '.' {
						r.part = string(epart)
						r.path = path[i+1:]
						r.more = true
						return
					} else if path[i] == '*' || path[i] == '?' {
						r.wild = true
					}
					epart = append(epart, path[i])
				}
			}
			// append the last part
			r.part = string(epart)
			return
		}
	}
	r.part = path
	return
}

func parseSquash(json string, i int) (int, string) {
	// expects that the lead character is a '[' or '{'
	// squash the value, ignoring all nested arrays and objects.
	// the first '[' or '{' has already been read
	s := i
	i++
	depth := 1
	for ; i < len(json); i++ {
		if json[i] >= '"' && json[i] <= '}' {
			switch json[i] {
			case '"':
				i++
				s2 := i
				for ; i < len(json); i++ {
					if json[i] > '\\' {
						continue
					}
					if json[i] == '"' {
						// look for an escaped slash
						if json[i-1] == '\\' {
							n := 0
							for j := i - 2; j > s2-1; j-- {
								if json[j] != '\\' {
									break
								}
								n++
							}
							if n%2 == 0 {
								continue
							}
						}
						break
					}
				}
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					i++
					return i, json[s:i]
				}
			}
		}
	}
	return i, json[s:]
}

func parseObject(c *parseContext, i int, path string) (int, bool) {
	var pmatch, kesc, vesc, ok, hit bool
	var key, val string
	rp := parseObjectPath(path)
	for i < len(c.json) {
		for ; i < len(c.json); i++ {
			if c.json[i] == '"' {
				// parse_key_string
				// this is slightly different from getting s string value
				// because we don't need the outer quotes.
				i++
				var s = i
				for ; i < len(c.json); i++ {
					if c.json[i] > '\\' {
						continue
					}
					if c.json[i] == '"' {
						i, key, kesc, ok = i+1, c.json[s:i], false, true
						goto parse_key_string_done
					}
					if c.json[i] == '\\' {
						i++
						for ; i < len(c.json); i++ {
							if c.json[i] > '\\' {
								continue
							}
							if c.json[i] == '"' {
								// look for an escaped slash
								if c.json[i-1] == '\\' {
									n := 0
									for j := i - 2; j > 0; j-- {
										if c.json[j] != '\\' {
											break
										}
										n++
									}
									if n%2 == 0 {
										continue
									}
								}
								i, key, kesc, ok = i+1, c.json[s:i], true, true
								goto parse_key_string_done
							}
						}
						break
					}
				}
				i, key, kesc, ok = i, c.json[s:], false, false
			parse_key_string_done:
				break
			}
			if c.json[i] == '}' {
				return i + 1, false
			}
		}
		if !ok {
			return i, false
		}
		if rp.wild {
			if kesc {
				pmatch = match.Match(unescape(key), rp.part)
			} else {
				pmatch = match.Match(key, rp.part)
			}
		} else {
			if kesc {
				pmatch = rp.part == unescape(key)
			} else {
				pmatch = rp.part == key
			}
		}
		hit = pmatch && !rp.more
		for ; i < len(c.json); i++ {
			switch c.json[i] {
			default:
				continue
			case '"':
				i++
				i, val, vesc, ok = parseString(c.json, i)
				if !ok {
					return i, false
				}
				if hit {
					if vesc {
						c.value.Str = unescape(val[1 : len(val)-1])
					} else {
						c.value.Str = val[1 : len(val)-1]
					}
					c.value.Raw = val
					c.value.Type = String
					return i, true
				}
			case '{':
				if pmatch && !hit {
					i, hit = parseObject(c, i+1, rp.path)
					if hit {
						return i, true
					}
				} else {
					i, val = parseSquash(c.json, i)
					if hit {
						c.value.Raw = val
						c.value.Type = JSON
						return i, true
					}
				}
			case '[':
				if pmatch && !hit {
					i, hit = parseArray(c, i+1, rp.path)
					if hit {
						return i, true
					}
				} else {
					i, val = parseSquash(c.json, i)
					if hit {
						c.value.Raw = val
						c.value.Type = JSON
						return i, true
					}
				}
			case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				i, val = parseNumber(c.json, i)
				if hit {
					c.value.Raw = val
					c.value.Type = Number
					c.value.Num, _ = strconv.ParseFloat(val, 64)
					return i, true
				}
			case 't', 'f', 'n':
				vc := c.json[i]
				i, val = parseLiteral(c.json, i)
				if hit {
					c.value.Raw = val
					switch vc {
					case 't':
						c.value.Type = True
					case 'f':
						c.value.Type = False
					}
					return i, true
				}
			}
			break
		}
	}
	return i, false
}
func queryMatches(rp *arrayPathResult, value Result) bool {
	rpv := rp.query.value
	if len(rpv) > 2 && rpv[0] == '"' && rpv[len(rpv)-1] == '"' {
		rpv = rpv[1 : len(rpv)-1]
	}
	switch value.Type {
	case String:
		switch rp.query.op {
		case "=":
			return value.Str == rpv
		case "!=":
			return value.Str != rpv
		case "<":
			return value.Str < rpv
		case "<=":
			return value.Str <= rpv
		case ">":
			return value.Str > rpv
		case ">=":
			return value.Str >= rpv
		case "%":
			return match.Match(value.Str, rpv)
		}
	case Number:
		rpvn, _ := strconv.ParseFloat(rpv, 64)
		switch rp.query.op {
		case "=":
			return value.Num == rpvn
		case "!=":
			return value.Num == rpvn
		case "<":
			return value.Num < rpvn
		case "<=":
			return value.Num <= rpvn
		case ">":
			return value.Num > rpvn
		case ">=":
			return value.Num >= rpvn
		}
	case True:
		switch rp.query.op {
		case "=":
			return rpv == "true"
		case "!=":
			return rpv != "true"
		case ">":
			return rpv == "false"
		case ">=":
			return true
		}
	case False:
		switch rp.query.op {
		case "=":
			return rpv == "false"
		case "!=":
			return rpv != "false"
		case "<":
			return rpv == "true"
		case "<=":
			return true
		}
	}
	return false
}
func parseArray(c *parseContext, i int, path string) (int, bool) {
	var pmatch, vesc, ok, hit bool
	var val string
	var h int
	var alog []int
	var partidx int
	var multires []byte
	rp := parseArrayPath(path)
	if !rp.arrch {
		n, err := strconv.ParseUint(rp.part, 10, 64)
		if err != nil {
			partidx = -1
		} else {
			partidx = int(n)
		}
	}
	for i < len(c.json) {
		if !rp.arrch {
			pmatch = partidx == h
			hit = pmatch && !rp.more
		}
		h++
		if rp.alogok {
			alog = append(alog, i)
		}
		for ; i < len(c.json); i++ {
			switch c.json[i] {
			default:
				continue
			case '"':
				i++
				i, val, vesc, ok = parseString(c.json, i)
				if !ok {
					return i, false
				}
				if hit {
					if rp.alogok {
						break
					}
					if vesc {
						c.value.Str = unescape(val[1 : len(val)-1])
					} else {
						c.value.Str = val[1 : len(val)-1]
					}
					c.value.Raw = val
					c.value.Type = String
					return i, true
				}
			case '{':
				if pmatch && !hit {
					i, hit = parseObject(c, i+1, rp.path)
					if hit {
						if rp.alogok {
							break
						}
						return i, true
					}
				} else {
					i, val = parseSquash(c.json, i)
					if rp.query.on {
						res := Get(val, rp.query.path)
						if queryMatches(&rp, res) {
							if rp.more {
								res = Get(val, rp.path)
							} else {
								res = Result{Raw: val, Type: JSON}
							}
							if rp.query.all {
								if len(multires) == 0 {
									multires = append(multires, '[')
								} else {
									multires = append(multires, ',')
								}
								multires = append(multires, res.Raw...)
							} else {
								c.value = res
								return i, true
							}
						}
					} else if hit {
						if rp.alogok {
							break
						}
						c.value.Raw = val
						c.value.Type = JSON
						return i, true
					}
				}
			case '[':
				if pmatch && !hit {
					i, hit = parseArray(c, i+1, rp.path)
					if hit {
						if rp.alogok {
							break
						}
						return i, true
					}
				} else {
					i, val = parseSquash(c.json, i)
					if hit {
						if rp.alogok {
							break
						}
						c.value.Raw = val
						c.value.Type = JSON
						return i, true
					}
				}
			case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				i, val = parseNumber(c.json, i)
				if hit {
					if rp.alogok {
						break
					}
					c.value.Raw = val
					c.value.Type = Number
					c.value.Num, _ = strconv.ParseFloat(val, 64)
					return i, true
				}
			case 't', 'f', 'n':
				vc := c.json[i]
				i, val = parseLiteral(c.json, i)
				if hit {
					if rp.alogok {
						break
					}
					c.value.Raw = val
					switch vc {
					case 't':
						c.value.Type = True
					case 'f':
						c.value.Type = False
					}
					return i, true
				}
			case ']':
				if rp.arrch && rp.part == "#" {
					if rp.alogok {
						var jsons = make([]byte, 0, 64)
						jsons = append(jsons, '[')
						for j, k := 0, 0; j < len(alog); j++ {
							res := Get(c.json[alog[j]:], rp.alogkey)
							if res.Exists() {
								if k > 0 {
									jsons = append(jsons, ',')
								}
								jsons = append(jsons, []byte(res.Raw)...)
								k++
							}
						}
						jsons = append(jsons, ']')
						c.value.Type = JSON
						c.value.Raw = string(jsons)
						return i + 1, true
					} else {
						if rp.alogok {
							break
						}
						c.value.Raw = val
						c.value.Type = Number
						c.value.Num = float64(h - 1)
						c.calcd = true
						return i + 1, true
					}
				}
				if len(multires) > 0 && !c.value.Exists() {
					c.value = Result{
						Raw:  string(append(multires, ']')),
						Type: JSON,
					}
				}
				return i + 1, false
			}
			break
		}
	}
	return i, false
}

type parseContext struct {
	json  string
	value Result
	calcd bool
}

// Get searches json for the specified path.
// A path is in dot syntax, such as "name.last" or "age".
// This function expects that the json is well-formed, and does not validate.
// Invalid json will not panic, but it may return back unexpected results.
// When the value is found it's returned immediately.
//
// A path is a series of keys searated by a dot.
// A key may contain special wildcard characters '*' and '?'.
// To access an array value use the index as the key.
// To get the number of elements in an array or to access a child path, use the '#' character.
// The dot and wildcard character can be escaped with '\'.
//
//  {
//    "name": {"first": "Tom", "last": "Anderson"},
//    "age":37,
//    "children": ["Sara","Alex","Jack"],
//    "friends": [
//      {"first": "James", "last": "Murphy"},
//      {"first": "Roger", "last": "Craig"}
//    ]
//  }
//  "name.last"          >> "Anderson"
//  "age"                >> 37
//  "children"           >> ["Sara","Alex","Jack"]
//  "children.#"         >> 3
//  "children.1"         >> "Alex"
//  "child*.2"           >> "Jack"
//  "c?ildren.0"         >> "Sara"
//  "friends.#.first"    >> ["James","Roger"]
//
func Get(json, path string) Result {
	var i int
	var c = &parseContext{json: json}
	for ; i < len(c.json); i++ {
		if c.json[i] == '{' {
			i++
			parseObject(c, i, path)
			break
		}
		if c.json[i] == '[' {
			i++
			parseArray(c, i, path)
			break
		}
	}
	if len(c.value.Raw) > 0 && !c.calcd {
		jhdr := *(*reflect.StringHeader)(unsafe.Pointer(&json))
		rhdr := *(*reflect.StringHeader)(unsafe.Pointer(&(c.value.Raw)))
		c.value.Index = int(rhdr.Data - jhdr.Data)
		if c.value.Index < 0 || c.value.Index >= len(json) {
			c.value.Index = 0
		}
	}
	return c.value
}
func fromBytesGet(result Result) Result {
	// safely get the string headers
	rawhi := *(*reflect.StringHeader)(unsafe.Pointer(&result.Raw))
	strhi := *(*reflect.StringHeader)(unsafe.Pointer(&result.Str))
	// create byte slice headers
	rawh := reflect.SliceHeader{Data: rawhi.Data, Len: rawhi.Len}
	strh := reflect.SliceHeader{Data: strhi.Data, Len: strhi.Len}
	if strh.Data == 0 {
		// str is nil
		if rawh.Data == 0 {
			// raw is nil
			result.Raw = ""
		} else {
			// raw has data, safely copy the slice header to a string
			result.Raw = string(*(*[]byte)(unsafe.Pointer(&rawh)))
		}
		result.Str = ""
	} else if rawh.Data == 0 {
		// raw is nil
		result.Raw = ""
		// str has data, safely copy the slice header to a string
		result.Str = string(*(*[]byte)(unsafe.Pointer(&strh)))
	} else if strh.Data >= rawh.Data &&
		int(strh.Data)+strh.Len <= int(rawh.Data)+rawh.Len {
		// Str is a substring of Raw.
		start := int(strh.Data - rawh.Data)
		// safely copy the raw slice header
		result.Raw = string(*(*[]byte)(unsafe.Pointer(&rawh)))
		// substring the raw
		result.Str = result.Raw[start : start+strh.Len]
	} else {
		// safely copy both the raw and str slice headers to strings
		result.Raw = string(*(*[]byte)(unsafe.Pointer(&rawh)))
		result.Str = string(*(*[]byte)(unsafe.Pointer(&strh)))
	}
	return result
}

// GetBytes searches json for the specified path.
// If working with bytes, this method preferred over Get(string(data), path)
func GetBytes(json []byte, path string) Result {
	var result Result
	if json != nil {
		// unsafe cast to string
		result = Get(*(*string)(unsafe.Pointer(&json)), path)
		result = fromBytesGet(result)
	}
	return result
}

// unescape unescapes a string
func unescape(json string) string { //, error) {
	var str = make([]byte, 0, len(json))
	for i := 0; i < len(json); i++ {
		switch {
		default:
			str = append(str, json[i])
		case json[i] < ' ':
			return "" //, errors.New("invalid character in string")
		case json[i] == '\\':
			i++
			if i >= len(json) {
				return "" //, errors.New("invalid escape sequence")
			}
			switch json[i] {
			default:
				return "" //, errors.New("invalid escape sequence")
			case '\\':
				str = append(str, '\\')
			case '/':
				str = append(str, '/')
			case 'b':
				str = append(str, '\b')
			case 'f':
				str = append(str, '\f')
			case 'n':
				str = append(str, '\n')
			case 'r':
				str = append(str, '\r')
			case 't':
				str = append(str, '\t')
			case '"':
				str = append(str, '"')
			case 'u':
				if i+5 > len(json) {
					return "" //, errors.New("invalid escape sequence")
				}
				i++
				// extract the codepoint
				var code int
				for j := i; j < i+4; j++ {
					switch {
					default:
						return "" //, errors.New("invalid escape sequence")
					case json[j] >= '0' && json[j] <= '9':
						code += (int(json[j]) - '0') << uint(12-(j-i)*4)
					case json[j] >= 'a' && json[j] <= 'f':
						code += (int(json[j]) - 'a' + 10) << uint(12-(j-i)*4)
					case json[j] >= 'a' && json[j] <= 'f':
						code += (int(json[j]) - 'a' + 10) << uint(12-(j-i)*4)
					}
				}
				str = append(str, []byte(string(code))...)
				i += 3 // only 3 because we will increment on the for-loop
			}
		}
	}
	return string(str) //, nil
}

// Less return true if a token is less than another token.
// The caseSensitive paramater is used when the tokens are Strings.
// The order when comparing two different type is:
//
//  Null < False < Number < String < True < JSON
//
func (t Result) Less(token Result, caseSensitive bool) bool {
	if t.Type < token.Type {
		return true
	}
	if t.Type > token.Type {
		return false
	}
	if t.Type == String {
		if caseSensitive {
			return t.Str < token.Str
		}
		return stringLessInsensitive(t.Str, token.Str)
	}
	if t.Type == Number {
		return t.Num < token.Num
	}
	return t.Raw < token.Raw
}

func stringLessInsensitive(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}

// parseAny parses the next value from a json string.
// A Result is returned when the hit param is set.
// The return values are (i int, res Result, ok bool)
func parseAny(json string, i int, hit bool) (int, Result, bool) {
	var res Result
	var val string
	for ; i < len(json); i++ {
		if json[i] == '{' || json[i] == '[' {
			i, val = parseSquash(json, i)
			if hit {
				res.Raw = val
				res.Type = JSON
			}
			return i, res, true
		}
		if json[i] <= ' ' {
			continue
		}
		switch json[i] {
		case '"':
			i++
			var vesc bool
			var ok bool
			i, val, vesc, ok = parseString(json, i)
			if !ok {
				return i, res, false
			}
			if hit {
				res.Type = String
				res.Raw = val
				if vesc {
					res.Str = unescape(val[1 : len(val)-1])
				} else {
					res.Str = val[1 : len(val)-1]
				}
			}
			return i, res, true
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			i, val = parseNumber(json, i)
			if hit {
				res.Raw = val
				res.Type = Number
				res.Num, _ = strconv.ParseFloat(val, 64)
			}
			return i, res, true
		case 't', 'f', 'n':
			vc := json[i]
			i, val = parseLiteral(json, i)
			if hit {
				res.Raw = val
				switch vc {
				case 't':
					res.Type = True
				case 'f':
					res.Type = False
				}
				return i, res, true
			}
		}
	}
	return i, res, false
}

var ( // used for testing
	testWatchForFallback bool
	testLastWasFallback  bool
)

// areSimplePaths returns true if all the paths are simple enough
// to parse quickly for GetMany(). Allows alpha-numeric, dots,
// underscores, and the dollar sign. It does not allow non-alnum,
// escape characters, or keys which start with a numbers.
// For example:
//   "name.last" == OK
//   "user.id0" == OK
//   "user.ID" == OK
//   "user.first_name" == OK
//   "user.firstName" == OK
//   "user.0item" == BAD
//   "user.#id" == BAD
//   "user\.name" == BAD
func areSimplePaths(paths []string) bool {
	for _, path := range paths {
		var fi int // first key index, for keys with numeric prefix
		for i := 0; i < len(path); i++ {
			if path[i] >= 'a' && path[i] <= 'z' {
				// a-z is likely to be the highest frequency charater.
				continue
			}
			if path[i] == '.' {
				fi = i + 1
				continue
			}
			if path[i] >= 'A' && path[i] <= 'Z' {
				continue
			}
			if path[i] == '_' || path[i] == '$' {
				continue
			}
			if i > fi && path[i] >= '0' && path[i] <= '9' {
				continue
			}
			return false
		}
	}
	return true
}

// GetMany searches json for the multiple paths.
// The return value is a Result array where the number of items
// will be equal to the number of input paths.
func GetMany(json string, paths ...string) []Result {
	if len(paths) < 4 {
		if testWatchForFallback {
			testLastWasFallback = false
		}
		switch len(paths) {
		case 0:
			// return nil when no paths are specified.
			return nil
		case 1:
			return []Result{Get(json, paths[0])}
		case 2:
			return []Result{Get(json, paths[0]), Get(json, paths[1])}
		case 3:
			return []Result{Get(json, paths[0]), Get(json, paths[1]), Get(json, paths[2])}
		}
	}
	var results []Result
	var ok bool
	var i int
	if len(paths) > 512 {
		// we can only support up to 512 paths. Is that too many?
		goto fallback
	}
	if !areSimplePaths(paths) {
		// If there is even one path that is not considered "simple" then
		// we need to use the fallback method.
		goto fallback
	}
	// locate the object token.
	for ; i < len(json); i++ {
		if json[i] == '{' {
			i++
			break
		}
		if json[i] <= ' ' {
			continue
		}
		goto fallback
	}
	// use the call function table.
	if len(paths) <= 8 {
		results, ok = getMany8(json, i, paths)
	} else if len(paths) <= 16 {
		results, ok = getMany16(json, i, paths)
	} else if len(paths) <= 32 {
		results, ok = getMany32(json, i, paths)
	} else if len(paths) <= 64 {
		results, ok = getMany64(json, i, paths)
	} else if len(paths) <= 128 {
		results, ok = getMany128(json, i, paths)
	} else if len(paths) <= 256 {
		results, ok = getMany256(json, i, paths)
	} else if len(paths) <= 512 {
		results, ok = getMany512(json, i, paths)
	}
	if !ok {
		// there was some fault while parsing. we should try the
		// fallback method. This could result in performance
		// degregation in some cases.
		goto fallback
	}
	if testWatchForFallback {
		testLastWasFallback = false
	}
	return results
fallback:
	results = results[:0]
	for i := 0; i < len(paths); i++ {
		results = append(results, Get(json, paths[i]))
	}
	if testWatchForFallback {
		testLastWasFallback = true
	}
	return results
}

// GetManyBytes searches json for the specified path.
// If working with bytes, this method preferred over
// GetMany(string(data), paths...)
func GetManyBytes(json []byte, paths ...string) []Result {
	if json == nil {
		return GetMany("", paths...)
	}
	results := GetMany(*(*string)(unsafe.Pointer(&json)), paths...)
	for i := range results {
		results[i] = fromBytesGet(results[i])
	}
	return results
}

// parseGetMany parses a json object for keys that match against the callers
// paths. It's a best-effort attempt and quickly locating and assigning the
// values to the []Result array. If there are failures such as bad json, or
// invalid input paths, or too much recursion, the function will exit with a
// return value of 'false'.
func parseGetMany(
	json string, i int,
	level uint, kplen int,
	paths []string, completed []bool, matches []uint64, results []Result,
) (int, bool) {
	if level > 62 {
		// The recursion level is limited because the matches []uint64
		// array cannot handle more the 64-bits.
		return i, false
	}
	// At this point the last character read was a '{'.
	// Read all object keys and try to match against the paths.
	var key string
	var val string
	var vesc, ok bool
next_key:
	for ; i < len(json); i++ {
		if json[i] == '"' {
			// read the key
			i, val, vesc, ok = parseString(json, i+1)
			if !ok {
				return i, false
			}
			if vesc {
				// the value is escaped
				key = unescape(val[1 : len(val)-1])
			} else {
				// just a plain old ascii key
				key = val[1 : len(val)-1]
			}
			var hasMatch bool
			var parsedVal bool
			var valOrgIndex int
			var valPathIndex int
			for j := 0; j < len(key); j++ {
				if key[j] == '.' {
					// we need to look for keys with dot and ignore them.
					if i, _, ok = parseAny(json, i, false); !ok {
						return i, false
					}
					continue next_key
				}
			}
			var usedPaths int
			// loop through paths and look for matches
			for j := 0; j < len(paths); j++ {
				if completed[j] {
					usedPaths++
					// ignore completed paths
					continue
				}
				if level > 0 && (matches[j]>>(level-1))&1 == 0 {
					// ignore unmatched paths
					usedPaths++
					continue
				}

				// try to match the key to the path
				// this is spaghetti code but the idea is to minimize
				// calls and variable assignments when comparing the
				// key to paths
				if len(paths[j])-kplen >= len(key) {
					i, k := kplen, 0
					for ; k < len(key); k, i = k+1, i+1 {
						if key[k] != paths[j][i] {
							// no match
							goto nomatch
						}
					}
					if i < len(paths[j]) {
						if paths[j][i] == '.' {
							// matched, but there still more keys in the path
							goto match_not_atend
						}
					}
					// matched and at the end of the path
					goto match_atend
				}
				// no match, jump to the nomatch label
				goto nomatch
			match_atend:
				// found a match
				// at the end of the path. we must take the value.
				usedPaths++
				if !parsedVal {
					// the value has not been parsed yet. let's do so.
					valOrgIndex = i // keep track of the current position.
					i, results[j], ok = parseAny(json, i, true)
					if !ok {
						return i, false
					}
					parsedVal = true
					valPathIndex = j
				} else {
					results[j] = results[valPathIndex]
				}
				// mark as complete
				completed[j] = true
				// jump over the match_not_atend label
				goto nomatch
			match_not_atend:
				// found a match
				// still in the middle of the path.
				usedPaths++
				// mark the path as matched
				matches[j] |= 1 << level
				if !hasMatch {
					hasMatch = true
				}
			nomatch: // noop label
			}

			if !parsedVal {
				if hasMatch {
					// we found a match and the value has not been parsed yet.
					// let's find out if the next value type is an object.
					for ; i < len(json); i++ {
						if json[i] <= ' ' || json[i] == ':' {
							continue
						}
						break
					}
					if i < len(json) {
						if json[i] == '{' {
							// it's an object. let's go deeper
							i, ok = parseGetMany(json, i+1, level+1, kplen+len(key)+1, paths, completed, matches, results)
							if !ok {
								return i, false
							}
						} else {
							// not an object. just parse and ignore.
							if i, _, ok = parseAny(json, i, false); !ok {
								return i, false
							}
						}
					}
				} else {
					// Since there was no matches we can just parse the value and
					// ignore the result.
					if i, _, ok = parseAny(json, i, false); !ok {
						return i, false
					}
				}
			} else if hasMatch && len(results[valPathIndex].Raw) > 0 && results[valPathIndex].Raw[0] == '{' {
				// The value was already parsed and the value type is an object.
				// Rewind the json index and let's parse deeper.
				i = valOrgIndex
				for ; i < len(json); i++ {
					if json[i] == '{' {
						break
					}
				}
				i, ok = parseGetMany(json, i+1, level+1, kplen+len(key)+1, paths, completed, matches, results)
				if !ok {
					return i, false
				}
			}
			if usedPaths == len(paths) {
				// all paths have been used, either completed or matched.
				// we should stop parsing this object to save CPU cycles.
				if level > 0 && i < len(json) {
					i, _ = parseSquash(json, i)
				}
				return i, true
			}
		} else if json[i] == '}' {
			// reached the end of the object. end it here.
			return i + 1, true
		}
	}
	return i, true
}

// Call table for GetMany. Using an isolated function allows for allocating
// arrays with know capacities on the stack, as opposed to dynamically
// allocating on the heap. This can provide a tremendous performance boost
// by avoiding the GC.
func getMany8(json string, i int, paths []string) ([]Result, bool) {
	const max = 8
	var completed = make([]bool, 0, max)
	var matches = make([]uint64, 0, max)
	var results = make([]Result, 0, max)
	completed = completed[0:len(paths):max]
	matches = matches[0:len(paths):max]
	results = results[0:len(paths):max]
	_, ok := parseGetMany(json, i, 0, 0, paths, completed, matches, results)
	return results, ok
}
func getMany16(json string, i int, paths []string) ([]Result, bool) {
	const max = 16
	var completed = make([]bool, 0, max)
	var matches = make([]uint64, 0, max)
	var results = make([]Result, 0, max)
	completed = completed[0:len(paths):max]
	matches = matches[0:len(paths):max]
	results = results[0:len(paths):max]
	_, ok := parseGetMany(json, i, 0, 0, paths, completed, matches, results)
	return results, ok
}
func getMany32(json string, i int, paths []string) ([]Result, bool) {
	const max = 32
	var completed = make([]bool, 0, max)
	var matches = make([]uint64, 0, max)
	var results = make([]Result, 0, max)
	completed = completed[0:len(paths):max]
	matches = matches[0:len(paths):max]
	results = results[0:len(paths):max]
	_, ok := parseGetMany(json, i, 0, 0, paths, completed, matches, results)
	return results, ok
}
func getMany64(json string, i int, paths []string) ([]Result, bool) {
	const max = 64
	var completed = make([]bool, 0, max)
	var matches = make([]uint64, 0, max)
	var results = make([]Result, 0, max)
	completed = completed[0:len(paths):max]
	matches = matches[0:len(paths):max]
	results = results[0:len(paths):max]
	_, ok := parseGetMany(json, i, 0, 0, paths, completed, matches, results)
	return results, ok
}
func getMany128(json string, i int, paths []string) ([]Result, bool) {
	const max = 128
	var completed = make([]bool, 0, max)
	var matches = make([]uint64, 0, max)
	var results = make([]Result, 0, max)
	completed = completed[0:len(paths):max]
	matches = matches[0:len(paths):max]
	results = results[0:len(paths):max]
	_, ok := parseGetMany(json, i, 0, 0, paths, completed, matches, results)
	return results, ok
}
func getMany256(json string, i int, paths []string) ([]Result, bool) {
	const max = 256
	var completed = make([]bool, 0, max)
	var matches = make([]uint64, 0, max)
	var results = make([]Result, 0, max)
	completed = completed[0:len(paths):max]
	matches = matches[0:len(paths):max]
	results = results[0:len(paths):max]
	_, ok := parseGetMany(json, i, 0, 0, paths, completed, matches, results)
	return results, ok
}
func getMany512(json string, i int, paths []string) ([]Result, bool) {
	const max = 512
	var completed = make([]bool, 0, max)
	var matches = make([]uint64, 0, max)
	var results = make([]Result, 0, max)
	completed = completed[0:len(paths):max]
	matches = matches[0:len(paths):max]
	results = results[0:len(paths):max]
	_, ok := parseGetMany(json, i, 0, 0, paths, completed, matches, results)
	return results, ok
}
