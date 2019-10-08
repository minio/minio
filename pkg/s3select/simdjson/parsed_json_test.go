package simdjson

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

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
		dec, err := zstd.NewReader(nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(tt.ref, func(t *testing.T) {
			tap, err := ioutil.ReadFile(filepath.Join("testdata", tt.tape))
			if err != nil {
				t.Fatal(err)
			}
			tap, err = dec.DecodeAll(tap, nil)
			if err != nil {
				t.Fatal(err)
			}
			sb, err := ioutil.ReadFile(filepath.Join("testdata", tt.stringbuf))
			if err != nil {
				t.Fatal(err)
			}
			sb, err = dec.DecodeAll(sb, nil)
			if err != nil {
				t.Fatal(err)
			}

			pj, err := LoadTape(bytes.NewBuffer(tap), bytes.NewBuffer(sb))
			if err != nil {
				t.Fatal(err)
			}
			i := pj.Iter()
			t.Log(i.Next())
			t.Log(i.Next())
			for {
				typ := i.Next()
				if typ == TypeNone {
					break
				}
				in, err := i.Interface()
				t.Log(typ, in, err)
			}
		})
	}
}
