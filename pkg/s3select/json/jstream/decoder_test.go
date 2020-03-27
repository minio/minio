package jstream

import (
	"bytes"
	"testing"
)

func mkReader(s string) *bytes.Reader { return bytes.NewReader([]byte(s)) }

func TestDecoderSimple(t *testing.T) {
	var (
		counter int
		body    = `[{"bio":"bada bing bada boom","id":1,"name":"Charles","falseVal":false}]`
	)

	decoder := NewDecoder(mkReader(body), 1)

	metaValChan, err := decoder.Stream()
	if err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	for mv := range metaValChan {
		counter++
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}

	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
}

func TestDecoderNested(t *testing.T) {
	var (
		counter int
		body    = `{
  "1": {
    "bio": "bada bing bada boom",
    "id": 0,
    "name": "Roberto",
    "nested1": {
      "bio": "utf16 surrogate (\ud834\udcb2)\n\u201cutf 8\u201d",
      "id": 1.5,
      "name": "Roberto*Maestro",
      "nested2": { "nested2arr": [0,1,2], "nested3": {
        "nested4": { "depth": "recursion" }}
			}
		}
  },
  "2": {
    "nullfield": null,
    "id": -2
  }
}`
	)

	decoder := NewDecoder(mkReader(body), 2)

	metaValChan, err := decoder.Stream()
	if err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	for mv := range metaValChan {
		counter++
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}

	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
}

func TestDecoderFlat(t *testing.T) {
	var (
		counter int
		body    = `[
  "1st test string",
  "Roberto*Maestro", "Charles",
  0, null, false,
  1, 2.5
]`
		expected = []struct {
			Value     interface{}
			ValueType ValueType
		}{
			{
				"1st test string",
				String,
			},
			{
				"Roberto*Maestro",
				String,
			},
			{
				"Charles",
				String,
			},
			{
				0.0,
				Number,
			},
			{
				nil,
				Null,
			},
			{
				false,
				Boolean,
			},
			{
				1.0,
				Number,
			},
			{
				2.5,
				Number,
			},
		}
	)

	decoder := NewDecoder(mkReader(body), 1)

	metaValChan, err := decoder.Stream()
	if err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	for mv := range metaValChan {
		if mv.Value != expected[counter].Value {
			t.Fatalf("got %v, expected: %v", mv.Value, expected[counter])
		}
		if mv.ValueType != expected[counter].ValueType {
			t.Fatalf("got %v value type, expected: %v value type", mv.ValueType, expected[counter].ValueType)
		}
		counter++
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}

	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
}

func TestDecoderMultiDoc(t *testing.T) {
	var (
		counter int
		body    = `{ "bio": "bada bing bada boom", "id": 1, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 2, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 3, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 4, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 5, "name": "Charles" }
`
	)

	decoder := NewDecoder(mkReader(body), 0)

	metaValChan, err := decoder.Stream()
	if err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	for mv := range metaValChan {
		if mv.ValueType != Object {
			t.Fatalf("got %v value type, expected: Object value type", mv.ValueType)
		}
		counter++
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	if counter != 5 {
		t.Fatalf("expected 5 items, got %d", counter)
	}

	// test at depth level 1
	counter = 0
	kvcounter := 0
	decoder = NewDecoder(mkReader(body), 1)

	metaValChan, err = decoder.Stream()
	if err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	for mv := range metaValChan {
		switch mv.Value.(type) {
		case KV:
			kvcounter++
		default:
			counter++
		}
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	if kvcounter != 0 {
		t.Fatalf("expected 0 keyvalue items, got %d", kvcounter)
	}
	if counter != 15 {
		t.Fatalf("expected 15 items, got %d", counter)
	}

	// test at depth level 1 w/ emitKV
	counter = 0
	kvcounter = 0
	decoder = NewDecoder(mkReader(body), 1).EmitKV()

	metaValChan, err = decoder.Stream()
	if err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	for mv := range metaValChan {
		switch mv.Value.(type) {
		case KV:
			kvcounter++
		default:
			counter++
		}
		t.Logf("depth=%d offset=%d len=%d (%v)", mv.Depth, mv.Offset, mv.Length, mv.Value)
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("decoder error: %s", err)
	}
	if kvcounter != 15 {
		t.Fatalf("expected 15 keyvalue items, got %d", kvcounter)
	}
	if counter != 0 {
		t.Fatalf("expected 0 items, got %d", counter)
	}
}
