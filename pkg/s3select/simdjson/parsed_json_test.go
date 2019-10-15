package simdjson

import (
	"bytes"
	"encoding/json"
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
			tap, sb, ref := loadCompressed(t, tt.name)

			var tmp interface{} = map[string]interface{}{}
			if tt.array {
				tmp = []interface{}{}
			}
			var refJSON []byte
			err := json.Unmarshal(ref, &tmp)
			if err != nil {
				t.Fatal(err)
			}
			refJSON, err = json.MarshalIndent(tmp, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
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

			for {
				var next simdjson.Iter
				typ, err := i.NextIter(&next)
				if err != nil {
					t.Fatal(err)
				}
				switch typ {
				case simdjson.TypeNone:
					return
				case simdjson.TypeRoot:
					i = next
				case simdjson.TypeArray:
					arr, err := next.Array(nil)
					if err != nil {
						t.Fatal(err)
					}
					got, err := arr.Interface()
					if err != nil {
						t.Fatal(err)
					}
					b, err := json.MarshalIndent(got, "", "  ")
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(b, refJSON) {
						_ = ioutil.WriteFile(filepath.Join("testdata", tt.name+".want"), refJSON, os.ModePerm)
						_ = ioutil.WriteFile(filepath.Join("testdata", tt.name+".got"), b, os.ModePerm)
						t.Error("Content mismatch. Output dumped to testdata.")
					}

				case simdjson.TypeObject:
					obj, err := next.Object(nil)
					if err != nil {
						t.Fatal(err)
					}
					got, err := obj.Map(nil)
					if err != nil {
						t.Fatal(err)
					}
					b, err := json.MarshalIndent(got, "", "  ")
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(b, refJSON) {
						_ = ioutil.WriteFile(filepath.Join("testdata", tt.name+".want"), refJSON, os.ModePerm)
						_ = ioutil.WriteFile(filepath.Join("testdata", tt.name+".got"), b, os.ModePerm)
						t.Error("Content mismatch. Output dumped to testdata.")
					}
				}
			}
		})
	}
}
