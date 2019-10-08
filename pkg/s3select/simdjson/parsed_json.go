package simdjson

// COPIED: REMOVE WHEN STUFF IS EXPORTED

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"unsafe"
)

const JSONVALUEMASK = 0xffffffffffffff

type ParsedJson struct {
	Tape    []uint64
	Strings []byte
}

type Iter struct {
	// The tape where this iter start.
	tape ParsedJson

	// offset of the next entry to be decoded
	off int
	// current value
	cur uint64
	// current tag
	t Tag
}

// LoadTape will load the input from the supplied readers.
func LoadTape(tape, strings io.Reader) (*ParsedJson, error) {
	b, err := ioutil.ReadAll(tape)
	if err != nil {
		return nil, err
	}
	if len(b)&7 != 0 {
		return nil, errors.New("unexpected tape length, should be modulo 8 bytes")
	}
	dst := ParsedJson{
		Tape:    make([]uint64, len(b)/8),
		Strings: nil,
	}
	// Read tape
	for i := range dst.Tape {
		dst.Tape[i] = binary.LittleEndian.Uint64(b[i*8 : i*8+8])
	}
	// Read stringbuf
	b, err = ioutil.ReadAll(strings)
	if err != nil {
		return nil, err
	}
	dst.Strings = b
	return &dst, nil
}

func (pj *ParsedJson) Iter() Iter {
	return Iter{tape: *pj}
}

func (pj *ParsedJson) StringAt(offset uint64) (string, error) {
	offset &= JSONVALUEMASK
	// There must be at least 4 byte length and one 0 byte.
	if offset+5 > uint64(len(pj.Strings)) {
		return "", errors.New("string offset outside valid area")
	}
	length := uint64(binary.LittleEndian.Uint32(pj.Strings[offset : offset+4]))
	if offset+length > uint64(len(pj.Strings)) {
		return "", errors.New("string offset+length outside valid area")
	}
	if length <= 0 {
		return "", errors.New("string length was 0")
	}
	return string(pj.Strings[offset+4 : offset+4+length-1]), nil
}

// Next will read the type of the next element
// and queues up the value.
func (i *Iter) Next() Type {
	if i.off >= len(i.tape.Tape) {
		return TypeNone
	}
	v := i.tape.Tape[i.off]
	i.cur = v & JSONVALUEMASK
	i.t = Tag(v >> 56)
	i.off++
	return TagToType[i.t]
}

func (i *Iter) PeekNext() Type {
	if i.off >= len(i.tape.Tape) {
		return TypeNone
	}
	return TagToType[Tag(i.tape.Tape[i.off]>>56)]
}

// Float returns the float value of the next element.
// Integers are automatically converted to float.
func (i *Iter) Float() (float64, error) {
	switch i.t {
	case TagFloat:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected float, but no more values on tape")
		}
		v := math.Float64frombits(i.tape.Tape[i.off])
		i.off++
		return v, nil
	case TagInteger:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected integer, but no more values on tape")
		}
		v := int64(i.tape.Tape[i.off])
		i.off++
		return float64(v), nil
	case TagUint:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected integer, but no more values on tape")
		}
		v := i.tape.Tape[i.off]
		i.off++
		return float64(v), nil
	default:
		return 0, fmt.Errorf("unable to convert type %v to float", i.t)
	}
}

// Float returns the float value of the next element.
// Integers are automatically converted to float.
func (i *Iter) Int() (int64, error) {
	switch i.t {
	case TagFloat:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected float, but no more values on tape")
		}
		v := math.Float64frombits(i.tape.Tape[i.off])
		i.off++
		if v > math.MaxInt64 {
			return 0, errors.New("float value overflows int64")
		}
		if v < math.MinInt64 {
			return 0, errors.New("float value underflows int64")
		}
		return int64(v), nil
	case TagInteger:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected integer, but no more values on tape")
		}
		v := int64(i.tape.Tape[i.off])
		i.off++
		return v, nil
	case TagUint:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected integer, but no more values on tape")
		}
		v := i.tape.Tape[i.off]
		i.off++
		if v > math.MaxInt64 {
			return 0, errors.New("unsigned integer value overflows int64")
		}
		return int64(v), nil
	default:
		return 0, fmt.Errorf("unable to convert type %v to float", i.t)
	}
}

// Float returns the float value of the next element.
// Integers are automatically converted to float.
func (i *Iter) Uint() (uint64, error) {
	switch i.t {
	case TagFloat:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected float, but no more values on tape")
		}
		v := math.Float64frombits(i.tape.Tape[i.off])
		i.off++
		if v > math.MaxUint64 {
			return 0, errors.New("float value overflows uint64")
		}
		if v < 0 {
			return 0, errors.New("float value is negative. cannot convert to uint")
		}
		return uint64(v), nil
	case TagInteger:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected integer, but no more values on tape")
		}
		v := int64(i.tape.Tape[i.off])
		if v < 0 {
			return 0, errors.New("integer value is negative. cannot convert to uint")
		}

		i.off++
		return uint64(v), nil
	case TagUint:
		if i.off >= len(i.tape.Tape) {
			return 0, errors.New("corrupt input: expected integer, but no more values on tape")
		}
		v := i.tape.Tape[i.off]
		i.off++
		return v, nil
	default:
		return 0, fmt.Errorf("unable to convert type %v to float", i.t)
	}
}

// String() returns a string value.
func (i *Iter) String() (string, error) {
	if i.t != TagString {
		return "", errors.New("value is not string")
	}
	return i.tape.StringAt(i.cur)
}

// Bool() returns the bool value.
func (i *Iter) Bool() (bool, error) {
	switch i.t {
	case TagBoolTrue:
		return true, nil
	case TagBoolFalse:
		return false, nil
	}
	return false, fmt.Errorf("value is not bool, but %v", i.t)
}

// Interface returns the value as an interface.
func (i *Iter) Interface() (interface{}, error) {
	switch i.t.Type() {
	case TypeUint:
		return i.Uint()
	case TypeInt:
		return i.Int()
	case TypeFloat:
		return i.Float()
	case TypeNull:
		return nil, nil
	case TypeArray:
		arr, err := i.Array(nil)
		if err != nil {
			return nil, err
		}
		return arr.Interface()
	case TypeString:
		return i.String()
	case TypeObject:
		obj, err := i.Object(nil)
		if err != nil {
			return nil, err
		}
		return obj.Map(nil)
	case TypeBool:
		return i.t == TagBoolTrue, nil
	default:
	}
	return nil, fmt.Errorf("unknown tag type: %v", i.t)
}

type Object struct {
	// Complete tape
	tape ParsedJson

	// offset of the next entry to be decoded
	off int

	nameIdx map[string]int
}

// Object will return the next element as an object.
// An optional destination can be given.
func (i *Iter) Object(dst *Object) (*Object, error) {
	if i.t != TagObjectStart {
		return nil, errors.New("next item is not object")
	}
	end := i.cur
	if end < uint64(i.off) {
		return nil, errors.New("corrupt input: object ends at index before start")
	}
	if uint64(len(i.tape.Tape)) < end {
		return nil, errors.New("corrupt input: object extended beyond tape")
	}
	if dst == nil {
		dst = &Object{
			nameIdx: make(map[string]int),
		}
	} else {
		for k := range dst.nameIdx {
			delete(dst.nameIdx, k)
		}
	}
	dst.tape.Tape = i.tape.Tape[:end]
	dst.tape.Strings = i.tape.Strings
	dst.off = i.off

	return dst, nil
}

// Object will return the next element as an object.
// An optional destination can be given.
func (i *Iter) Array(dst *Array) (*Array, error) {
	if i.t != TagArrayStart {
		return nil, errors.New("next item is not object")
	}
	end := i.cur
	if uint64(len(i.tape.Tape)) < end {
		return nil, errors.New("corrupt input: object extended beyond tape")
	}
	if dst == nil {
		dst = &Array{}
	}
	dst.tape.Tape = i.tape.Tape[:end]
	dst.tape.Strings = i.tape.Strings
	dst.off = i.off

	return dst, nil
}

// Map will unmarshal into a map[string]interface{}
func (o *Object) Map(dst map[string]interface{}) (map[string]interface{}, error) {
	if dst == nil {
		dst = make(map[string]interface{})
	}
	var tmp Iter
	for {
		name, t, err := o.NextElement(&tmp)
		if err != nil {
			return nil, err
		}
		if t == TypeNone {
			// Done
			break
		}
		dst[name], err = tmp.Interface()
		if err != nil {
			return nil, fmt.Errorf("parsing element %q: %w", name, err)
		}
	}
	return dst, nil
}

// NextElement sets dst to the next element and returns the name.
// TypeNone with nil error will be returned if there are no more elements.
func (o *Object) NextElement(dst *Iter) (name string, t Type, err error) {
	if o.off == len(o.tape.Tape) {
		return "", TypeNone, nil
	}
	// Next must be string or end of object
	v := o.tape.Tape[o.off]

	// Read name:
	tag := Tag(v >> 56)

	switch tag {
	case TagString:
		name, err = o.tape.StringAt(v)
		if err != nil {
			return "", TypeNone, fmt.Errorf("parsing object element name: %w", err)
		}
		o.off++
	case TagObjectEnd:
		return "", TypeNone, nil
	default:
		return "", TypeNone, fmt.Errorf("object: unexpected tag %v", tag)
	}
	v = o.tape.Tape[o.off]
	o.nameIdx[name] = o.off
	tag = Tag(v >> 56)
	return name, TagToType[tag], nil
}

type Array struct {
	tape      ParsedJson
	off       int
	Len       int
	FirstType Type
}

func (a *Array) Interface() ([]interface{}, error) {
	// FIXME:
	return nil, nil
}

// AsFloat returns the array values as float.
// Integers are automatically converted to float.
func (a *Array) AsFloat() ([]float64, error) {
	// Estimate length
	lenEst := (len(a.tape.Tape) - a.off - 1) / 2
	if lenEst < 0 {
		lenEst = 0
	}
	dst := make([]float64, 0, lenEst)

readArray:
	for {
		tag := Tag(a.tape.Tape[a.off] >> 56)
		a.off++
		switch tag {
		case TagFloat:
			if len(a.tape.Tape) <= a.off {
				return nil, errors.New("corrupt input: expected float, but no more values")
			}
			dst = append(dst, math.Float64frombits(a.tape.Tape[a.off]))
		case TagInteger:
			if len(a.tape.Tape) <= a.off {
				return nil, errors.New("corrupt input: expected integer, but no more values")
			}
			dst = append(dst, float64(int64(a.tape.Tape[a.off])))
		case TagUint:
			if len(a.tape.Tape) <= a.off {
				return nil, errors.New("corrupt input: expected integer, but no more values")
			}
			dst = append(dst, float64(a.tape.Tape[a.off]))
		case TagArrayEnd:
			break readArray
		default:
			return nil, fmt.Errorf("unable to convert type %v to float", tag)
		}
		a.off++
	}
	return dst, nil
}

// AsFloat returns the array values as float.
// Integers are automatically converted to float.
func (a *Array) AsInteger() ([]int64, error) {
	// Estimate length
	lenEst := (len(a.tape.Tape) - a.off - 1) / 2
	if lenEst < 0 {
		lenEst = 0
	}
	dst := make([]int64, 0, lenEst)
readArray:
	for {
		tag := Tag(a.tape.Tape[a.off] >> 56)
		a.off++
		switch tag {
		case TagFloat:
			if len(a.tape.Tape) <= a.off {
				return nil, errors.New("corrupt input: expected float, but no more values")
			}
			val := math.Float64frombits(a.tape.Tape[a.off])
			if val > math.MaxInt64 {
				return nil, errors.New("float value overflows int64")
			}
			if val < math.MinInt64 {
				return nil, errors.New("float value underflows int64")
			}
			dst = append(dst, int64(val))
		case TagInteger:
			if len(a.tape.Tape) <= a.off {
				return nil, errors.New("corrupt input: expected integer, but no more values")
			}
			dst = append(dst, int64(a.tape.Tape[a.off]))
		case TagUint:
			if len(a.tape.Tape) <= a.off {
				return nil, errors.New("corrupt input: expected integer, but no more values")
			}

			val := a.tape.Tape[a.off]
			if val > math.MaxInt64 {
				return nil, errors.New("unsigned integer value overflows int64")
			}

			dst = append(dst)
		case TagArrayEnd:
			break readArray
		default:
			return nil, fmt.Errorf("unable to convert type %v to integer", tag)
		}
		a.off++
	}
	return dst, nil
}

func (pj *ParsedJson) loadFile(tape, stringBuf string) (*ParsedJson, error) {
	//tape := io.ReadFull()
	return nil, nil
}

func (pj *ParsedJson) Reset() {
	pj.Tape = pj.Tape[:0]
	pj.Strings = pj.Strings[:0]
}

func (pj *ParsedJson) get_current_loc() uint64 {
	return uint64(len(pj.Tape))
}

func (pj *ParsedJson) write_tape(val uint64, c byte) {
	pj.Tape = append(pj.Tape, val|(uint64(c)<<56))
}

func (pj *ParsedJson) write_tape_s64(val int64) {
	pj.write_tape(0, 'l')
	pj.Tape = append(pj.Tape, uint64(val))
}

func (pj *ParsedJson) write_tape_double(d float64) {
	pj.write_tape(0, 'd')
	pj.Tape = append(pj.Tape, float64_2_uint64(d))
}

func (pj *ParsedJson) annotate_previousloc(saved_loc uint64, val uint64) {
	pj.Tape[saved_loc] |= val
}

// Tag indicates the data type of a tape entry
type Tag uint8

const (
	TagString      = Tag('"')
	TagInteger     = Tag('l')
	TagUint        = Tag('u')
	TagFloat       = Tag('d')
	TagNull        = Tag('n')
	TagBoolTrue    = Tag('t')
	TagBoolFalse   = Tag('f')
	TagObjectStart = Tag('{')
	TagObjectEnd   = Tag('}')
	TagArrayStart  = Tag('[')
	TagArrayEnd    = Tag(']')
	TagRoot        = Tag('r')
	TagEnd         = Tag(0)
)

// Type is a JSON value type.
type Type uint8

const (
	TypeNone Type = iota
	TypeNull
	TypeString
	TypeInt
	TypeUint
	TypeFloat
	TypeBool
	TypeObject
	TypeArray
)

// TagToType converts a tag to type.
// For arrays and objects only the start tag will return types.
// All non-existing tags returns TypeNone.
var TagToType = [256]Type{
	TagString:      TypeString,
	TagInteger:     TypeInt,
	TagUint:        TypeUint,
	TagFloat:       TypeFloat,
	TagNull:        TypeNull,
	TagBoolTrue:    TypeBool,
	TagBoolFalse:   TypeBool,
	TagObjectStart: TypeObject,
	TagArrayStart:  TypeArray,
}

func (t Tag) Type() Type {
	return TagToType[t]
}

func (pj *ParsedJson) dump_raw_tape() bool {

	//if !pj.isvalid {
	//	return false
	// }

	tapeidx := uint64(0)
	howmany := uint64(0)
	tape_val := pj.Tape[tapeidx]
	ntype := tape_val >> 56
	fmt.Printf("%d : %s", tapeidx, string(ntype))

	if ntype == 'r' {
		howmany = tape_val & JSONVALUEMASK
	} else {
		fmt.Errorf("Error: no starting root node?\n")
		return false
	}
	fmt.Printf("\t// pointing to %d (right after last node)\n", howmany)

	tapeidx++
	for ; tapeidx < howmany; tapeidx++ {
		tape_val = pj.Tape[tapeidx]
		fmt.Printf("%d : ", tapeidx)
		ntype := Tag(tape_val >> 56)
		payload := tape_val & JSONVALUEMASK
		switch ntype {
		case TagString: // we have a string
			fmt.Printf("string \"")
			string_length := uint64(binary.LittleEndian.Uint32(pj.Strings[payload : payload+4]))
			fmt.Printf("%s", print_with_escapes(pj.Strings[payload+4:payload+4+string_length]))
			fmt.Println("\"")

		case TagInteger: // we have a long int
			if tapeidx+1 >= howmany {
				return false
			}
			tapeidx++
			fmt.Printf("integer %d\n", int64(pj.Tape[tapeidx]))

		case TagFloat: // we have a double
			if tapeidx+1 >= howmany {
				return false
			}
			tapeidx++
			fmt.Printf("float %f\n", Uint64toFloat64(pj.Tape[tapeidx]))

		case TagNull: // we have a null
			fmt.Printf("null\n")

		case TagBoolTrue: // we have a true
			fmt.Printf("true\n")

		case TagBoolFalse: // we have a false
			fmt.Printf("false\n")

		case TagObjectStart: // we have an object
			fmt.Printf("{\t// pointing to next Tape location %d (first node after the scope) \n", payload)

		case TagObjectEnd: // we end an object
			fmt.Printf("}\t// pointing to previous Tape location %d (start of the scope) \n", payload)

		case TagArrayStart: // we start an array
			fmt.Printf("\t// pointing to next Tape location %d (first node after the scope) \n", payload)

		case TagArrayEnd: // we end an array
			fmt.Printf("]\t// pointing to previous Tape location %d (start of the scope) \n", payload)

		case TagRoot: // we start and end with the root node
			fmt.Printf("end of root\n")
			return false

		default:
			return false
		}
	}

	tape_val = pj.Tape[tapeidx]
	payload := tape_val & JSONVALUEMASK
	ntype = tape_val >> 56
	fmt.Printf("%d : %s\t// pointing to %d (start root)\n", tapeidx, string(ntype), payload)

	return true
}

func Uint64toFloat64(i uint64) float64 {
	return *(*float64)(unsafe.Pointer(&i))
}

func Float64toUint64(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}

func print_with_escapes(src []byte) string {

	result := make([]byte, 0, len(src))

	for _, s := range src {
		switch s {
		case '\b':
			result = append(result, []byte{'\\', 'b'}...)

		case '\f':
			result = append(result, []byte{'\\', 'f'}...)

		case '\n':
			result = append(result, []byte{'\\', 'n'}...)

		case '\r':
			result = append(result, []byte{'\\', 'r'}...)

		case '"':
			result = append(result, []byte{'\\', '"'}...)

		case '\t':
			result = append(result, []byte{'\\', 't'}...)

		case '\\':
			result = append(result, []byte{'\\', '\\'}...)

		default:
			if s <= 0x1f {
				result = append(result, []byte(fmt.Sprintf("%04x", s))...)
			} else {
				result = append(result, s)
			}
		}
	}

	return string(result)
}
