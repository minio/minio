package jstream

import (
	"bufio"
	"bytes"
	"fmt"
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
	t.Skip("Unstable test")
	data := []byte("abcdefghijklmnopqrstuvwxyz0123456789")

	var i int
	r := bytes.NewReader(data)
	scanner := newScanner(r)
	for scanner.pos < atomic.LoadInt64(&scanner.end) {
		c := scanner.next()
		if scanner.readerErr != nil {
			t.Fatal(scanner.readerErr)
		}
		if c != data[i] {
			t.Fatalf("expected %s, got %s", string(data[i]), string(c))
		}
		t.Logf("pos=%d remaining=%d (%s)", i, r.Len(), string(c))
		i++
	}
}

type mockReader struct {
	pos       int
	mockData  byte
	failAfter int
}

func newMockReader(failAfter int, data byte) *mockReader {
	return &mockReader{0, data, failAfter}
}

func (r *mockReader) Read(p []byte) (n int, err error) {
	if r.pos >= r.failAfter {
		return 0, fmt.Errorf("intentionally unexpected reader error")
	}
	r.pos++
	p[0] = r.mockData
	return 1, nil
}

func TestScannerFailure(t *testing.T) {
	var (
		i         int
		failAfter = 900
		mockData  = byte(32)
	)

	r := newMockReader(failAfter, mockData)
	scanner := newScanner(r)

	for i < 1000 {
		c := scanner.next()
		if c == byte(0) {
			break
		}
		if c != mockData {
			t.Fatalf("expected \"%s\", got \"%s\"", string(mockData), string(c))
		}
		i++
	}
	c := scanner.next()
	if scanner.readerErr == nil {
		t.Fatalf("failed to receive expected error after %d bytes", failAfter)
	}
	if c != byte(0) {
		t.Fatalf("expected null byte, got %v", c)
	}
}

func BenchmarkBufioScanner(b *testing.B) {
	b.Run("small", func(b *testing.B) {
		for b.Loop() {
			benchmarkBufioScanner(smallInput)
		}
	})
	b.Run("medium", func(b *testing.B) {
		for b.Loop() {
			benchmarkBufioScanner(mediumInput)
		}
	})
	b.Run("large", func(b *testing.B) {
		for b.Loop() {
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
		for b.Loop() {
			benchmarkBufioReader(smallInput)
		}
	})
	b.Run("medium", func(b *testing.B) {
		for b.Loop() {
			benchmarkBufioReader(mediumInput)
		}
	})
	b.Run("large", func(b *testing.B) {
		for b.Loop() {
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
		for b.Loop() {
			benchmarkScanner(smallInput)
		}
	})
	b.Run("medium", func(b *testing.B) {
		for b.Loop() {
			benchmarkScanner(mediumInput)
		}
	})
	b.Run("large", func(b *testing.B) {
		for b.Loop() {
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
