package simdjson

// COPIED: REMOVE WHEN STUFF IS EXPORTED

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

const JSONVALUEMASK = 0xffffffffffffff

type ParsedJson struct {
	Tape    []uint64
	Strings []byte
}

type Iter struct {
	// The first entry in 'p' points to the first object at the current level.
	p ParsedJson
}

func (pj *ParsedJson) Iter() Iter {
	return Iter{p: *pj}
}

func (i *Iter) Next() (Type, string) {
	return 0, ""
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

type Tag uint8

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
)

type Type uint8

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
		ntype := tape_val >> 56
		payload := tape_val & JSONVALUEMASK
		switch ntype {
		case '"': // we have a string
			fmt.Printf("string \"")
			string_length := uint64(binary.LittleEndian.Uint32(pj.Strings[payload : payload+4]))
			fmt.Printf("%s", print_with_escapes(pj.Strings[payload+4:payload+4+string_length]))
			fmt.Println("\"")

		case 'l': // we have a long int
			if tapeidx+1 >= howmany {
				return false
			}
			tapeidx++
			fmt.Printf("integer %d\n", int64(pj.Tape[tapeidx]))

		case 'd': // we have a double
			if tapeidx+1 >= howmany {
				return false
			}
			tapeidx++
			fmt.Printf("float %f\n", Uint64toFloat64(pj.Tape[tapeidx]))

		case 'n': // we have a null
			fmt.Printf("null\n")

		case 't': // we have a true
			fmt.Printf("true\n")

		case 'f': // we have a false
			fmt.Printf("false\n")

		case '{': // we have an object
			fmt.Printf("{\t// pointing to next Tape location %d (first node after the scope) \n", payload)

		case '}': // we end an object
			fmt.Printf("}\t// pointing to previous Tape location %d (start of the scope) \n", payload)

		case '[': // we start an array
			fmt.Printf("\t// pointing to next Tape location %d (first node after the scope) \n", payload)

		case ']': // we end an array
			fmt.Printf("]\t// pointing to previous Tape location %d (start of the scope) \n", payload)

		case 'r': // we start and end with the root node
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
