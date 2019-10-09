package simdjson

// COPIED: REMOVE WHEN STUFF IS EXPORTED

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"strconv"
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

	// current value, exclude tag in top bits
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
	b, err := pj.StringByteAt(offset)
	return string(b), err
}

func (pj *ParsedJson) StringByteAt(offset uint64) ([]byte, error) {
	offset &= JSONVALUEMASK
	// There must be at least 4 byte length and one 0 byte.
	if offset+5 > uint64(len(pj.Strings)) {
		return nil, errors.New("string offset outside valid area")
	}
	length := uint64(binary.LittleEndian.Uint32(pj.Strings[offset : offset+4]))
	if offset+length > uint64(len(pj.Strings)) {
		return nil, errors.New("string offset+length outside valid area")
	}
	return pj.Strings[offset+4 : offset+4+length], nil
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

// NextIter will read the type of the next element
// and return an iterator only containing the object.
// The iterator 'i' is forwarded to the next value,
// meaning next call will return next object at this level.
// If dst and i are the same, both will contain the value inside.
func (i *Iter) NextIter(dst *Iter) (Type, error) {
	if i.off == len(i.tape.Tape) {
		return TypeNone, nil
	}
	if i.off > len(i.tape.Tape) {
		return TypeNone, errors.New("offset bigger than tape")
	}
	v := i.tape.Tape[i.off]
	i.cur = v & JSONVALUEMASK
	i.t = Tag(v >> 56)
	i.off++
	typ := TagToType[i.t]

	// copy i.
	if i != dst {
		*dst = *i
	}
	switch typ {
	case TypeRoot, TypeObject, TypeArray:
		if i.cur > uint64(len(i.tape.Tape)) {
			return TypeNone, errors.New("root element extends beyond tape")
		}
		dst.tape.Tape = dst.tape.Tape[:dst.cur]
	case TypeBool, TypeNull, TypeNone, TypeString:
		dst.tape.Tape = dst.tape.Tape[:dst.off]
	case TypeInt, TypeUint, TypeFloat:
		if i.off >= len(i.tape.Tape) {
			return TypeNone, errors.New("expected element value not on")
		}
		dst.tape.Tape = dst.tape.Tape[:dst.off+1]
	}
	if i != dst {
		// Forward i if different.
		i.off = len(dst.tape.Tape)
	}
	return typ, nil
}

func (i *Iter) PeekNext() Type {
	if i.off >= len(i.tape.Tape) {
		return TypeNone
	}
	return TagToType[Tag(i.tape.Tape[i.off]>>56)]
}

// PeekNextTag will return the tag at the current offset.
// Will return TagEnd if at end of iterator.
func (i *Iter) PeekNextTag() Tag {
	if i.off >= len(i.tape.Tape) {
		return TagEnd
	}
	return Tag(i.tape.Tape[i.off] >> 56)
}

// MarshalJSON will marshal the entire remaining scope of the iterator.
func (i *Iter) MarshalJSON() ([]byte, error) {
	return i.MarshalJSONBuffer(nil)
}

// MarshalJSONBuffer will marshal the entire remaining scope of the iterator.
// An optional buffer can be provided for fewer allocations.
// Output will be appended to the destination.
func (i *Iter) MarshalJSONBuffer(dst []byte) ([]byte, error) {
	var tmpBuf []byte

	// Pre-allocate for 100 deep.
	var stackTmp [100]uint8
	// We have a stackNone on top of the stack
	stack := stackTmp[:1]
	const (
		stackNone = iota
		stackArray
		stackObject
	)

	for {
		// Write key names.
		if stack[len(stack)-1] == stackObject && i.t != TagObjectEnd {
			sb, err := i.StringBytes()
			if err != nil {
				return nil, fmt.Errorf("expected key within object: %w", err)
			}
			tmpBuf = escapeBytes(tmpBuf, sb)
			dst = append(dst, '"')
			dst = append(dst, tmpBuf...)
			dst = append(dst, '"', ':')
			if i.PeekNextTag() == TagEnd {
				return nil, fmt.Errorf("unexpected end of tape within object")
			}
			i.Next()
		}
		//fmt.Println(string(i.t))
		switch i.t {
		case TagRoot:
			// Move into root.
			var err error
			i, err = i.Root()
			if err != nil {
				return nil, err
			}
		case TagString:
			sb, err := i.StringBytes()
			if err != nil {
				return nil, err
			}
			tmpBuf = escapeBytes(tmpBuf, sb)
			dst = append(dst, '"')
			dst = append(dst, tmpBuf...)
			dst = append(dst, '"')
		case TagInteger:
			v, err := i.Int()
			if err != nil {
				return nil, err
			}
			dst = append(dst, []byte(strconv.FormatInt(v, 10))...)
		case TagUint:
			v, err := i.Uint()
			if err != nil {
				return nil, err
			}
			dst = append(dst, []byte(strconv.FormatUint(v, 10))...)
		case TagFloat:
			v, err := i.Float()
			if err != nil {
				return nil, err
			}
			dst = append(dst, []byte(strconv.FormatFloat(v, 'g', -1, 64))...)
		case TagNull:
			dst = append(dst, []byte("null")...)
		case TagBoolTrue:
			dst = append(dst, []byte("true")...)
		case TagBoolFalse:
			dst = append(dst, []byte("false")...)
		case TagObjectStart:
			dst = append(dst, '{')
			stack = append(stack, stackObject)
			// We should not emit commas.
			i.Next()
			continue
		case TagObjectEnd:
			dst = append(dst, '}')
			if stack[len(stack)-1] != stackObject {
				return dst, errors.New("end of object with no object on stack")
			}
			stack = stack[:len(stack)-1]
		case TagArrayStart:
			dst = append(dst, '[')
			stack = append(stack, stackArray)
			i.Next()
			continue
		case TagArrayEnd:
			dst = append(dst, ']')
			if stack[len(stack)-1] != stackArray {
				return nil, errors.New("end of object with no array on stack")
			}
			stack = stack[:len(stack)-1]
		}

		if i.PeekNextTag() == TagEnd {
			break
		}
		i.Next()

		// Output object separators, etc.
		switch stack[len(stack)-1] {
		case stackArray:
			switch i.t {
			case TagArrayEnd:
			default:
				dst = append(dst, ',')
			}
		case stackObject:
			switch i.t {
			case TagObjectEnd:
			default:
				dst = append(dst, ',')
			}
		}
	}
	if len(stack) > 1 {
		return nil, fmt.Errorf("objects or arrays not closed. left on stack: %v", stack[1:])
	}
	return dst, nil
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

// String() returns a string value.
func (i *Iter) StringBytes() ([]byte, error) {
	if i.t != TagString {
		return nil, errors.New("value is not string")
	}
	return i.tape.StringByteAt(i.cur)
}

// Root() returns the object embedded in root as an iterator.
// The iterator is moved to the first element after the root object.
func (i *Iter) Root() (*Iter, error) {
	if i.t != TagRoot {
		return nil, errors.New("value is not string")
	}
	if i.cur > uint64(len(i.tape.Tape)) {
		return nil, errors.New("root element extends beyond tape")
	}
	dst := Iter{}
	dst = *i
	dst.tape.Tape = dst.tape.Tape[:i.cur]
	i.off = int(i.cur)
	return &dst, nil
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
	//fmt.Println("interface type:", i.t.Type())
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
	case TypeRoot:
		// Skip root
		obj, err := i.Root()
		if err != nil {
			return nil, err
		}
		return obj.Interface()
	default:
	}
	return nil, fmt.Errorf("unknown tag type: %v", i.t)
}

type Object struct {
	// Complete tape
	tape ParsedJson

	// offset of the next entry to be decoded
	off int
}

// Object will return the next element as an object.
// An optional destination can be given.
// 'i' is forwarded to the end of the object.
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
		dst = &Object{}
	}
	dst.tape.Tape = i.tape.Tape[:end]
	dst.tape.Strings = i.tape.Strings
	dst.off = i.off
	i.off = int(end)

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
	i.off = int(end)

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
		//fmt.Println("Map name:", name, "type:", t)
		if t == TypeNone {
			// Done
			break
		}
		dst[name], err = tmp.Interface()
		//fmt.Println("value:", dst[name], "err:", err)
		if err != nil {
			return nil, fmt.Errorf("parsing element %q: %w", name, err)
		}
	}
	return dst, nil
}

type Element struct {
	Name string
	Type Type
	Iter Iter
}

type Elements struct {
	Elements []Element
	Index    map[string]int
}

// Parse will return all elements and iterators for each.
// An optional destination can be given.
// The Object will be consumed.
func (o *Object) Parse(dst *Elements) (*Elements, error) {
	if dst == nil {
		dst = &Elements{
			Elements: make([]Element, 0, 5),
			Index:    make(map[string]int, 5),
		}
	} else {
		dst.Elements = dst.Elements[:0]
		for k := range dst.Index {
			delete(dst.Index, k)
		}
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
		dst.Index[name] = len(dst.Elements)
		dst.Elements = append(dst.Elements, Element{
			Name: name,
			Type: t,
			Iter: tmp,
		})
	}
	return dst, nil
}

// NextElement sets dst to the next element and returns the name.
// TypeNone with nil error will be returned if there are no more elements.
func (o *Object) NextElement(dst *Iter) (name string, t Type, err error) {
	if o.off >= len(o.tape.Tape) {
		return "", TypeNone, nil
	}
	// Next must be string or end of object
	v := o.tape.Tape[o.off]
	switch Tag(v >> 56) {
	case TagString:
		// Read name:
		name, err = o.tape.StringAt(v)
		if err != nil {
			return "", TypeNone, fmt.Errorf("parsing object element name: %w", err)
		}
		o.off++
	case TagObjectEnd:
		return "", TypeNone, nil
	default:
		return "", TypeNone, fmt.Errorf("object: unexpected tag %v", string(v>>56))
	}

	// Read element type
	v = o.tape.Tape[o.off]
	// Move to value (if any)
	o.off++

	// Set dst
	dst.cur = v & JSONVALUEMASK
	dst.t = Tag(v >> 56)
	dst.off = o.off
	dst.tape = o.tape

	// Queue first element in dst.
	switch dst.t.Type() {
	case TypeRoot, TypeObject, TypeArray:
		if dst.cur > uint64(len(o.tape.Tape)) {
			return "", TypeNone, errors.New("element extends beyond tape")
		}
		dst.tape.Tape = dst.tape.Tape[:dst.cur]
	case TypeBool, TypeNull, TypeNone, TypeString:
		dst.tape.Tape = dst.tape.Tape[:dst.off]
	case TypeInt, TypeUint, TypeFloat:
		if dst.off >= len(dst.tape.Tape) {
			return "", TypeNone, errors.New("expected element value not on")
		}
		dst.tape.Tape = dst.tape.Tape[:dst.off+1]
	}

	// Skip to next element
	o.off = len(dst.tape.Tape)
	return name, TagToType[dst.t], nil
}

type Array struct {
	tape ParsedJson
	off  int
	Len  int
}

// Iter returns the array as an iterator.
// This can be used for parsing mixed content arrays.
// The first value is ready with a call to Next.
// Calling after last element should have TypeNone.
func (a *Array) Iter() Iter {
	i := Iter{
		tape: a.tape,
		off:  a.off,
	}
	return i
}

func (a *Array) Interface() ([]interface{}, error) {
	// Estimate length. Assume one value per element.
	lenEst := (len(a.tape.Tape) - a.off - 1) / 2
	if lenEst < 0 {
		lenEst = 0
	}
	dst := make([]interface{}, 0, lenEst)
	i := a.Iter()
	for i.Next() != TypeNone {
		elem, err := i.Interface()
		if err != nil {
			return nil, err
		}
		dst = append(dst, elem)
	}
	return dst, nil
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
	TypeRoot
)

func (t Type) String() string {
	switch t {
	case TypeNone:
		return "(no type)"
	case TypeNull:
		return "null"
	case TypeString:
		return "string"
	case TypeInt:
		return "int"
	case TypeUint:
		return "uint"
	case TypeFloat:
		return "float"
	case TypeBool:
		return "bool"
	case TypeObject:
		return "object"
	case TypeArray:
		return "array"
	case TypeRoot:
		return "root"
	}
	return "(invalid)"
}

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
	TagRoot:        TypeRoot,
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
	return string(escapeBytes(nil, src))
}

func escapeBytes(dst, src []byte) []byte {
	if cap(dst) < len(src) {
		dst = make([]byte, 0, len(src)+len(src)>>4)
	}
	dst = dst[:0]

	for _, s := range src {
		switch s {
		case '\b':
			dst = append(dst, '\\', 'b')

		case '\f':
			dst = append(dst, '\\', 'f')

		case '\n':
			dst = append(dst, '\\', 'n')

		case '\r':
			dst = append(dst, '\\', 'r')

		case '"':
			dst = append(dst, '\\', '"')

		case '\t':
			dst = append(dst, '\\', 't')

		case '\\':
			dst = append(dst, '\\', '\\')

		default:
			if s <= 0x1f {
				dst = append(dst, '\\', 'u', '0', '0', valToHex[s>>4], valToHex[s&0xf])
			} else {
				dst = append(dst, s)
			}
		}
	}

	return dst
}

var valToHex = [16]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}
