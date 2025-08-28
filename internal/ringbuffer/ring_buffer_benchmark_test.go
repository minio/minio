package ringbuffer

import (
	"io"
	"strings"
	"testing"
)

func BenchmarkRingBuffer_Sync(b *testing.B) {
	rb := New(1024)
	data := []byte(strings.Repeat("a", 512))
	buf := make([]byte, 512)

	for b.Loop() {
		rb.Write(data)
		rb.Read(buf)
	}
}

func BenchmarkRingBuffer_AsyncRead(b *testing.B) {
	// Pretty useless benchmark, but it's here for completeness.
	rb := New(1024)
	data := []byte(strings.Repeat("a", 512))
	buf := make([]byte, 512)

	go func() {
		for {
			rb.Read(buf)
		}
	}()

	for b.Loop() {
		rb.Write(data)
	}
}

func BenchmarkRingBuffer_AsyncReadBlocking(b *testing.B) {
	const sz = 512
	const buffers = 10
	rb := New(sz * buffers)
	rb.SetBlocking(true)
	data := []byte(strings.Repeat("a", sz))
	buf := make([]byte, sz)

	go func() {
		for {
			rb.Read(buf)
		}
	}()

	for b.Loop() {
		rb.Write(data)
	}
}

func BenchmarkRingBuffer_AsyncWrite(b *testing.B) {
	rb := New(1024)
	data := []byte(strings.Repeat("a", 512))
	buf := make([]byte, 512)

	go func() {
		for {
			rb.Write(data)
		}
	}()

	for b.Loop() {
		rb.Read(buf)
	}
}

func BenchmarkRingBuffer_AsyncWriteBlocking(b *testing.B) {
	const sz = 512
	const buffers = 10
	rb := New(sz * buffers)
	rb.SetBlocking(true)
	data := []byte(strings.Repeat("a", sz))
	buf := make([]byte, sz)

	go func() {
		for {
			rb.Write(data)
		}
	}()

	for b.Loop() {
		rb.Read(buf)
	}
}

func BenchmarkIoPipeReader(b *testing.B) {
	pr, pw := io.Pipe()
	data := []byte(strings.Repeat("a", 512))
	buf := make([]byte, 512)

	go func() {
		for {
			pw.Write(data)
		}
	}()

	for b.Loop() {
		pr.Read(buf)
	}
}
