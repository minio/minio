package simdjson

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/fwessels/simdjson-go"

	"github.com/klauspost/compress/zstd"
)

type tester interface {
	Fatal(args ...interface{})
}

func loadCompressed(t tester, file string) (tape, sb, ref []byte) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	tap, err := ioutil.ReadFile(filepath.Join("testdata", file+".tape.zst"))
	if err != nil {
		t.Fatal(err)
	}
	tap, err = dec.DecodeAll(tap, nil)
	if err != nil {
		t.Fatal(err)
	}
	sb, err = ioutil.ReadFile(filepath.Join("testdata", file+".stringbuf.zst"))
	if err != nil {
		t.Fatal(err)
	}
	sb, err = dec.DecodeAll(sb, nil)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ioutil.ReadFile(filepath.Join("testdata", file+".json.zst"))
	if err != nil {
		t.Fatal(err)
	}
	ref, err = dec.DecodeAll(ref, nil)
	if err != nil {
		t.Fatal(err)
	}

	return tap, sb, ref
}

var testCases = []struct {
	name  string
	array bool
}{
	{
		name: "parking-citations-10",
	},
}

func TestLoadTape(t *testing.T) {
	for _, tt := range testCases {

		t.Run(tt.name, func(t *testing.T) {
			tap, sb, _ := loadCompressed(t, tt.name)

			var err error
			dst := make(chan simdjson.Elements, 100)
			dec := NewElementReader(dst, &err, &ReaderArgs{unmarshaled: true, ContentType: "json"})
			pj, err := simdjson.LoadTape(bytes.NewBuffer(tap), bytes.NewBuffer(sb))
			if err != nil {
				t.Fatal(err)
			}
			i := pj.Iter()
			cpy := i
			b, err := cpy.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			if false {
				t.Log(string(b))
			}
			//_ = ioutil.WriteFile(filepath.Join("testdata", tt.name+".json"), b, os.ModePerm)

		parser:
			for {
				var next simdjson.Iter
				typ, err := i.NextIter(&next)
				if err != nil {
					t.Fatal(err)
				}
				switch typ {
				case simdjson.TypeNone:
					close(dst)
					break parser
				case simdjson.TypeRoot:
					obj, err := next.Root()
					if err != nil {
						t.Fatal(err)
					}
					if typ := obj.Next(); typ != simdjson.TypeObject {
						t.Fatal("Unexpected type:", typ.String())
					}

					o, err := obj.Object(nil)
					if err != nil {
						t.Fatal(err)
					}
					elems, err := o.Parse(nil)
					if err != nil {
						t.Fatal(err)
					}
					b, err := elems.MarshalJSON()
					t.Log(string(b))
					dst <- *elems
				default:
					t.Fatal("unexpected type:", typ.String())
				}
			}
			for {
				rec, err := dec.Read(nil)
				if err == io.EOF {
					break
				}
				if err != nil {
					continue
				}
				t.Log(rec.WriteCSV(os.Stdout, ','))
			}

		})
	}
}
