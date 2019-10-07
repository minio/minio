package simdjson

// COPIED: REMOVE WHEN STUFF IS EXPORTED

import (
	"encoding/binary"
	"errors"
	"fmt"
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
	// Next tag
	t Tag
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

func (i *Iter) Next() Type {
	if i.off >= len(i.tape.Tape) {
		return TypeNone
	}
	tag := Tag(i.tape.Tape[0] >> 56)
	switch tag {

	}
	i.t = tag
	return TagToType[tag]
}

type Object struct {
	// Complete tape
	tape ParsedJson

	// offset of the next entry to be decoded
	off int

	nameIdx map[string]int
}

func (i *Iter) Object() (*Object, error) {
	if i.t != TagObjectStart {
		return nil, errors.New("next item is not object")
	}
	v := i.tape.Tape[i.off]
	end := v & JSONVALUEMASK
	if uint64(len(i.tape.Tape)) < end {
		return nil, errors.New("corrupt input: object extended beyond tape")
	}
	i.off++
	o := Object{
		tape:    ParsedJson{Tape: i.tape.Tape[:end], Strings: i.tape.Strings},
		off:     i.off,
		nameIdx: nil,
	}

	return &o, nil
}

func (o *Object) NextElement() (name string, t Type, err error) {
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
	tag = Tag(v >> 56)
	return name, TagToType[tag], nil
}

type Array struct {
	p         ParsedJson
	off       int
	Len       int
	FirstType Type
}

func (a *Array) AsFloat() ([]float64, error) {

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

// Type is a JSON value type.
type Type uint8

const (
	TagString      = Tag('"')
	TagInteger     = Tag('l')
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

	TypeNone Type = iota
	TypeNull
	TypeString
	TypeInteger
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
	TagInteger:     TypeInteger,
	TagFloat:       TypeFloat,
	TagNull:        TypeNull,
	TagBoolTrue:    TypeBool,
	TagBoolFalse:   TypeBool,
	TagObjectStart: TypeObject,
	TagArrayStart:  TypeArray,
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
		ntype := Type(tape_val >> 56)
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
