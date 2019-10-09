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

func loadCompressed(t *testing.T, file string) []byte {
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

func TestLoadTape(t *testing.T) {
	var testCases = []struct {
		ref, tape, stringbuf string
	}{
		{
			ref:       "apache_builds.json.zst",
			tape:      "apache_builds.tape.zst",
			stringbuf: "apache_builds.stringbuf.zst",
		},
	}
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
