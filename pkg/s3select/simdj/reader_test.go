/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package simdj

import (
	"bytes"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/fwessels/simdjson-go"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio/pkg/s3select/json"
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

			var err error
			dst := make(chan simdjson.Elements, 100)
			dec := NewElementReader(dst, &err, &json.ReaderArgs{ContentType: "json"})
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
				typ, err := i.AdvanceIter(&next)
				if err != nil {
					t.Fatal(err)
				}
				switch typ {
				case simdjson.TypeNone:
					close(dst)
					break parser
				case simdjson.TypeRoot:
					obj, err := next.Root(nil)
					if err != nil {
						t.Fatal(err)
					}
					if typ := obj.Advance(); typ != simdjson.TypeObject {
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

					dst <- *elems
				default:
					t.Fatal("unexpected type:", typ.String())
				}
			}
			refDec := json.NewReader(ioutil.NopCloser(bytes.NewBuffer(ref)), &json.ReaderArgs{ContentType: "json"})

			for {
				rec, err := dec.Read(nil)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
				}
				want, err := refDec.Read(nil)
				if err != nil {
					t.Error(err)
				}
				var gotB, wantB bytes.Buffer
				err = rec.WriteCSV(&gotB, ',')
				if err != nil {
					t.Error(err)
				}
				err = want.WriteCSV(&wantB, ',')
				if err != nil {
					t.Error(err)
				}

				if !bytes.Equal(gotB.Bytes(), wantB.Bytes()) {
					t.Errorf("CSV output mismatch.\nwant: %s(%x)\ngot:  %s(%x)", wantB.String(), wantB.Bytes(), gotB.String(), gotB.Bytes())
				}
				gotB.Reset()
				wantB.Reset()

				err = rec.WriteJSON(&gotB)
				if err != nil {
					t.Error(err)
				}
				err = want.WriteJSON(&wantB)
				if err != nil {
					t.Error(err)
				}
				// truncate newline from 'want'
				wantB.Truncate(wantB.Len() - 1)
				if !bytes.Equal(gotB.Bytes(), wantB.Bytes()) {
					t.Errorf("JSON output mismatch.\nwant: %s\ngot:  %s", wantB.String(), gotB.String())
				}

			}

		})
	}
}
