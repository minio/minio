package simdjson

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

type tester interface {
	Fatal(args ...interface{})
}

func loadCompressed(t tester, file string) []byte {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	tap, err := ioutil.ReadFile(filepath.Join("testdata", file))
	if err != nil {
		t.Fatal(err)
	}
	tap, err = dec.DecodeAll(tap, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tap
}

var testCases = []struct {
	ref, tape, stringbuf string
}{
	{
		ref:       "apache_builds.json.zst",
		tape:      "apache_builds.tape.zst",
		stringbuf: "apache_builds.stringbuf.zst",
	},
	{
		ref:       "citm_catalog.json.zst",
		tape:      "citm_catalog.tape.zst",
		stringbuf: "citm_catalog.stringbuf.zst",
	},
	{
		ref:       "github_events.json.zst",
		tape:      "github_events.tape.zst",
		stringbuf: "github_events.stringbuf.zst",
	},
	{
		ref:       "gsoc-2018.json.zst",
		tape:      "gsoc-2018.tape.zst",
		stringbuf: "gsoc-2018.stringbuf.zst",
	},
	{
		ref:       "instruments.json.zst",
		tape:      "instruments.tape.zst",
		stringbuf: "instruments.stringbuf.zst",
	},
	{
		ref:       "numbers.json.zst",
		tape:      "numbers.tape.zst",
		stringbuf: "numbers.stringbuf.zst",
	},
	{
		ref:       "random.json.zst",
		tape:      "random.tape.zst",
		stringbuf: "random.stringbuf.zst",
	},
	{
		ref:       "update-center.json.zst",
		tape:      "update-center.tape.zst",
		stringbuf: "update-center.stringbuf.zst",
	},
}

func TestLoadTape(t *testing.T) {
	for _, tt := range testCases {

		t.Run(tt.ref, func(t *testing.T) {
			tap := loadCompressed(t, tt.tape)
			sb := loadCompressed(t, tt.stringbuf)
			ref := loadCompressed(t, tt.ref)

			var refMap map[string]interface{}
			err := json.Unmarshal(ref, &refMap)
			if err != nil {
				t.Fatal(err)
			}
			refJSON, err := json.MarshalIndent(refMap, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			pj, err := LoadTape(bytes.NewBuffer(tap), bytes.NewBuffer(sb))
			if err != nil {
				t.Fatal(err)
			}
			i := pj.Iter()
			cpy := i
			b, err := cpy.MarshalJSON()
			t.Log(string(b), err)
			_ = ioutil.WriteFile(filepath.Join("testdata", tt.ref+".json"), b, os.ModePerm)

			for {
				var next Iter
				typ, err := i.NextIter(&next)
				if err != nil {
					t.Fatal(err)
				}
				switch typ {
				case TypeNone:
					return
				case TypeRoot:
					i = next
				case TypeObject:
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
						_ = ioutil.WriteFile(filepath.Join("testdata", tt.ref+".want"), refJSON, os.ModePerm)
						_ = ioutil.WriteFile(filepath.Join("testdata", tt.ref+".got"), b, os.ModePerm)
						t.Error("Content mismatch. Output dumped to testdata.")
					}
				}
			}
		})
	}
}

func BenchmarkIter_MarshalJSONBuffer(b *testing.B) {
	for _, tt := range testCases {
		b.Run(tt.ref, func(b *testing.B) {
			tap := loadCompressed(b, tt.tape)
			sb := loadCompressed(b, tt.stringbuf)

			pj, err := LoadTape(bytes.NewBuffer(tap), bytes.NewBuffer(sb))
			if err != nil {
				b.Fatal(err)
			}
			iter := pj.Iter()
			cpy := iter
			output, err := cpy.MarshalJSON()
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(output)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cpy := iter
				output, err = cpy.MarshalJSONBuffer(output[:0])
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
