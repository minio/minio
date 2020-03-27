package jstream

import (
	"bufio"
	"bytes"
	"io"
	"sync/atomic"
	"testing"
)

var (
	smallInput  = make([]byte, 1024*12)       // 12K
	mediumInput = make([]byte, 1024*1024*12)  // 12MB
	largeInput  = make([]byte, 1024*1024*128) // 128MB
)

func TestScanner(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz0123456789")

	var i int
	r := bytes.NewReader(data)
	scanner := newScanner(r)
	for scanner.pos < atomic.LoadInt64(&scanner.end) {
		c, err := scanner.next()
		if err != nil {
			t.Fatal(err)
		}
		if c != data[i] {
			t.Fatalf("expected %s, got %s", string(data[i]), string(c))
		}
		t.Logf("pos=%d remaining=%d (%s)", i, r.Len(), string(c))
		i++
	}

}

func BenchmarkBufioScanner(b *testing.B) {
	b.Run("small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkBufioScanner(smallInput)
		}
	})
	b.Run("medium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkBufioScanner(mediumInput)
		}
	})
	b.Run("large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkBufioScanner(largeInput)
		}
	})
}

func benchmarkBufioScanner(b []byte) {
	s := bufio.NewScanner(bytes.NewReader(b))
	s.Split(bufio.ScanBytes)
	for s.Scan() {
		s.Bytes()
	}
}

func BenchmarkBufioReader(b *testing.B) {
	b.Run("small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkBufioReader(smallInput)
		}
	})
	b.Run("medium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkBufioReader(mediumInput)
		}
	})
	b.Run("large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkBufioReader(largeInput)
		}
	})
}

func benchmarkBufioReader(b []byte) {
	br := bufio.NewReader(bytes.NewReader(b))
loop:
	for {
		_, err := br.ReadByte()
		switch err {
		case nil:
			continue loop
		case io.EOF:
			break loop
		default:
			panic(err)
		}
	}
}

func BenchmarkScanner(b *testing.B) {
	b.Run("small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkScanner(smallInput)
		}
	})
	b.Run("medium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkScanner(mediumInput)
		}
	})
	b.Run("large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmarkScanner(largeInput)
		}
	})
}

func benchmarkScanner(b []byte) {
	r := bytes.NewReader(b)

	scanner := newScanner(r)
	for scanner.remaining() > 0 {
		scanner.next()
	}
}
