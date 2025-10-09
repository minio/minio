package ringbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRingBuffer_interface(t *testing.T) {
	rb := New(1)
	var _ io.Writer = rb
	var _ io.Reader = rb
	// var _ io.StringWriter = rb
	var _ io.ByteReader = rb
	var _ io.ByteWriter = rb
}

func TestRingBuffer_Write(t *testing.T) {
	rb := New(64)

	// check empty or full
	if !rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is true but got false")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}
	if rb.Length() != 0 {
		t.Fatalf("expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 64 {
		t.Fatalf("expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}

	// write 4 * 4 = 16 bytes
	n, err := rb.Write([]byte(strings.Repeat("abcd", 4)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 16 {
		t.Fatalf("expect write 16 bytes but got %d", n)
	}
	if rb.Length() != 16 {
		t.Fatalf("expect len 16 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 48 {
		t.Fatalf("expect free 48 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if !bytes.Equal(rb.Bytes(nil), []byte(strings.Repeat("abcd", 4))) {
		t.Fatalf("expect 4 abcd but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}

	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}

	// write 48 bytes, should full
	n, err = rb.Write([]byte(strings.Repeat("abcd", 12)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 48 {
		t.Fatalf("expect write 48 bytes but got %d", n)
	}
	if rb.Length() != 64 {
		t.Fatalf("expect len 64 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if rb.w != 0 {
		t.Fatalf("expect r.w=0 but got %d. r.r=%d", rb.w, rb.r)
	}
	if !bytes.Equal(rb.Bytes(nil), []byte(strings.Repeat("abcd", 16))) {
		t.Fatalf("expect 16 abcd but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}

	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if !rb.IsFull() {
		t.Fatalf("expect IsFull is true but got false")
	}

	// write more 4 bytes, should reject
	n, err = rb.Write([]byte(strings.Repeat("abcd", 1)))
	if err == nil {
		t.Fatalf("expect an error but got nil. n=%d, r.w=%d, r.r=%d", n, rb.w, rb.r)
	}
	if err != ErrIsFull {
		t.Fatalf("expect ErrIsFull but got nil")
	}
	if n != 0 {
		t.Fatalf("expect write 0 bytes but got %d", n)
	}
	if rb.Length() != 64 {
		t.Fatalf("expect len 64 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}

	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if !rb.IsFull() {
		t.Fatalf("expect IsFull is true but got false")
	}

	// reset this ringbuffer and set a long slice
	rb.Reset()
	n, err = rb.Write([]byte(strings.Repeat("abcd", 20)))
	if err == nil {
		t.Fatalf("expect ErrTooManyDataToWrite but got nil")
	}
	if n != 64 {
		t.Fatalf("expect write 64 bytes but got %d", n)
	}
	if rb.Length() != 64 {
		t.Fatalf("expect len 64 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if rb.w != 0 {
		t.Fatalf("expect r.w=0 but got %d. r.r=%d", rb.w, rb.r)
	}

	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if !rb.IsFull() {
		t.Fatalf("expect IsFull is true but got false")
	}

	if !bytes.Equal(rb.Bytes(nil), []byte(strings.Repeat("abcd", 16))) {
		t.Fatalf("expect 16 abcd but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}

	rb.Reset()
	// write 4 * 2 = 8 bytes
	n, err = rb.Write([]byte(strings.Repeat("abcd", 2)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 8 {
		t.Fatalf("expect write 16 bytes but got %d", n)
	}
	if rb.Length() != 8 {
		t.Fatalf("expect len 16 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 56 {
		t.Fatalf("expect free 48 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	buf := make([]byte, 5)
	rb.Read(buf)
	if rb.Length() != 3 {
		t.Fatalf("expect len 3 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	rb.Write([]byte(strings.Repeat("abcd", 15)))

	if !bytes.Equal(rb.Bytes(nil), []byte("bcd"+strings.Repeat("abcd", 15))) {
		t.Fatalf("expect 63 ... but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}

	rb.Reset()
	n, err = rb.Write([]byte(strings.Repeat("abcd", 16)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 64 {
		t.Fatalf("expect write 64 bytes but got %d", n)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	buf = make([]byte, 16)
	rb.Read(buf)
	n, err = rb.Write([]byte(strings.Repeat("1234", 4)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 16 {
		t.Fatalf("expect write 16 bytes but got %d", n)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if !bytes.Equal(append(buf, rb.Bytes(nil)...), []byte(strings.Repeat("abcd", 16)+strings.Repeat("1234", 4))) {
		t.Fatalf("expect 16 abcd and 4 1234 but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}
}

func TestRingBuffer_WriteBlocking(t *testing.T) {
	rb := New(64).SetBlocking(true)

	// check empty or full
	if !rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is true but got false")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}
	if rb.Length() != 0 {
		t.Fatalf("expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 64 {
		t.Fatalf("expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}

	// write 4 * 4 = 16 bytes
	n, err := rb.Write([]byte(strings.Repeat("abcd", 4)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 16 {
		t.Fatalf("expect write 16 bytes but got %d", n)
	}
	if rb.Length() != 16 {
		t.Fatalf("expect len 16 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 48 {
		t.Fatalf("expect free 48 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if !bytes.Equal(rb.Bytes(nil), []byte(strings.Repeat("abcd", 4))) {
		t.Fatalf("expect 4 abcd but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}

	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}

	// write 48 bytes, should full
	n, err = rb.Write([]byte(strings.Repeat("abcd", 12)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 48 {
		t.Fatalf("expect write 48 bytes but got %d", n)
	}
	if rb.Length() != 64 {
		t.Fatalf("expect len 64 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if rb.w != 0 {
		t.Fatalf("expect r.w=0 but got %d. r.r=%d", rb.w, rb.r)
	}
	if !bytes.Equal(rb.Bytes(nil), []byte(strings.Repeat("abcd", 16))) {
		t.Fatalf("expect 16 abcd but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}

	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if !rb.IsFull() {
		t.Fatalf("expect IsFull is true but got false")
	}

	rb.Reset()
	// write 4 * 2 = 8 bytes
	n, err = rb.Write([]byte(strings.Repeat("abcd", 2)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 8 {
		t.Fatalf("expect write 16 bytes but got %d", n)
	}
	if rb.Length() != 8 {
		t.Fatalf("expect len 16 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 56 {
		t.Fatalf("expect free 48 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	buf := make([]byte, 5)
	rb.Read(buf)
	if rb.Length() != 3 {
		t.Fatalf("expect len 3 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	rb.Write([]byte(strings.Repeat("abcd", 15)))

	if !bytes.Equal(rb.Bytes(nil), []byte("bcd"+strings.Repeat("abcd", 15))) {
		t.Fatalf("expect 63 ... but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}

	rb.Reset()
	n, err = rb.Write([]byte(strings.Repeat("abcd", 16)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 64 {
		t.Fatalf("expect write 64 bytes but got %d", n)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	buf = make([]byte, 16)
	rb.Read(buf)
	n, err = rb.Write([]byte(strings.Repeat("1234", 4)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 16 {
		t.Fatalf("expect write 16 bytes but got %d", n)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if !bytes.Equal(append(buf, rb.Bytes(nil)...), []byte(strings.Repeat("abcd", 16)+strings.Repeat("1234", 4))) {
		t.Fatalf("expect 16 abcd and 4 1234 but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}
}

func TestRingBuffer_Read(t *testing.T) {
	defer timeout(5 * time.Second)()
	rb := New(64)

	// check empty or full
	if !rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is true but got false")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}
	if rb.Length() != 0 {
		t.Fatalf("expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 64 {
		t.Fatalf("expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}

	// read empty
	buf := make([]byte, 1024)
	n, err := rb.Read(buf)
	if err == nil {
		t.Fatalf("expect an error but got nil")
	}
	if err != ErrIsEmpty {
		t.Fatalf("expect ErrIsEmpty but got nil")
	}
	if n != 0 {
		t.Fatalf("expect read 0 bytes but got %d", n)
	}
	if rb.Length() != 0 {
		t.Fatalf("expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 64 {
		t.Fatalf("expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if rb.r != 0 {
		t.Fatalf("expect r.r=0 but got %d. r.w=%d", rb.r, rb.w)
	}

	// write 16 bytes to read
	rb.Write([]byte(strings.Repeat("abcd", 4)))
	n, err = rb.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != 16 {
		t.Fatalf("expect read 16 bytes but got %d", n)
	}
	if rb.Length() != 0 {
		t.Fatalf("expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 64 {
		t.Fatalf("expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if rb.r != 16 {
		t.Fatalf("expect r.r=16 but got %d. r.w=%d", rb.r, rb.w)
	}

	// write long slice to  read
	rb.Write([]byte(strings.Repeat("abcd", 20)))
	n, err = rb.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != 64 {
		t.Fatalf("expect read 64 bytes but got %d", n)
	}
	if rb.Length() != 0 {
		t.Fatalf("expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 64 {
		t.Fatalf("expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if rb.r != 16 {
		t.Fatalf("expect r.r=16 but got %d. r.w=%d", rb.r, rb.w)
	}
}

func TestRingBuffer_Blocking(t *testing.T) {
	// Typical runtime is ~5-10s.
	defer timeout(60 * time.Second)()
	const debug = false

	var readBytes int
	var wroteBytes int
	var readBuf bytes.Buffer
	var wroteBuf bytes.Buffer
	readHash := crc32.NewIEEE()
	wroteHash := crc32.NewIEEE()
	read := io.Writer(readHash)
	wrote := io.Writer(wroteHash)
	if debug {
		read = io.MultiWriter(read, &readBuf)
		wrote = io.MultiWriter(wrote, &wroteBuf)
	}
	debugln := func(args ...any) {
		if debug {
			fmt.Println(args...)
		}
	}
	// Inject random reader/writer sleeps.
	const maxSleep = int(1 * time.Millisecond)
	doSleep := !testing.Short()
	rb := New(4 << 10).SetBlocking(true)

	// Reader
	var readErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		readRng := rand.New(rand.NewSource(1))
		defer wg.Done()
		defer rb.CloseWithError(readErr)
		buf := make([]byte, 1024)
		for {
			// Read
			n, err := rb.Read(buf[:readRng.Intn(len(buf))])
			readBytes += n
			read.Write(buf[:n])
			debugln("READ 1\t", n, readBytes)
			if err != nil {
				readErr = err
				break
			}

			// ReadByte
			b, err := rb.ReadByte()
			if err != nil {
				readErr = err
				break
			}
			readBytes++
			read.Write([]byte{b})
			debugln("READ 2\t", 1, readBytes)

			// TryRead
			n, err = rb.TryRead(buf[:readRng.Intn(len(buf))])
			readBytes += n
			read.Write(buf[:n])
			debugln("READ 3\t", n, readBytes)
			if err != nil && err != ErrAcquireLock && err != ErrIsEmpty {
				readErr = err
				break
			}
			if doSleep && readRng.Intn(20) == 0 {
				time.Sleep(time.Duration(readRng.Intn(maxSleep)))
			}
		}
	}()

	// Writer
	{
		buf := make([]byte, 1024)
		writeRng := rand.New(rand.NewSource(2))
		for range 2500 {
			writeRng.Read(buf)
			// Write
			n, err := rb.Write(buf[:writeRng.Intn(len(buf))])
			if err != nil {
				t.Fatalf("write failed: %v", err)
			}
			wroteBytes += n
			wrote.Write(buf[:n])
			debugln("WRITE 1\t", n, wroteBytes)

			// WriteString
			n, err = rb.WriteString(string(buf[:writeRng.Intn(len(buf))]))
			if err != nil {
				t.Fatalf("write failed: %v", err)
			}
			wroteBytes += n
			wrote.Write(buf[:n])
			debugln("WRITE 2\t", writeRng.Intn(len(buf)), wroteBytes)

			// WriteByte
			err = rb.WriteByte(buf[0])
			if err != nil {
				t.Fatalf("write failed: %v", err)
			}
			wroteBytes++
			wrote.Write(buf[:1])
			debugln("WRITE 3\t", 1, wroteBytes)

			// TryWrite
			n, err = rb.TryWrite(buf[:writeRng.Intn(len(buf))])
			if err != nil && err != ErrAcquireLock && err != ErrTooMuchDataToWrite && err != ErrIsFull {
				t.Fatalf("write failed: %v", err)
			}
			wroteBytes += n
			wrote.Write(buf[:n])
			debugln("WRITE 4\t", n, wroteBytes)

			// TryWriteByte
			err = rb.TryWriteByte(buf[0])
			if err != nil && err != ErrAcquireLock && err != ErrTooMuchDataToWrite && err != ErrIsFull {
				t.Fatalf("write failed: %v", err)
			}
			if err == nil {
				wroteBytes++
				wrote.Write(buf[:1])
				debugln("WRITE 5\t", 1, wroteBytes)
			}
			if doSleep && writeRng.Intn(10) == 0 {
				time.Sleep(time.Duration(writeRng.Intn(maxSleep)))
			}
		}
		if err := rb.Flush(); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
		rb.CloseWriter()
	}
	wg.Wait()
	if !errors.Is(readErr, io.EOF) {
		t.Fatalf("expect io.EOF but got %v", readErr)
	}
	if readBytes != wroteBytes {
		a, b := readBuf.Bytes(), wroteBuf.Bytes()
		if debug && !bytes.Equal(a, b) {
			common := len(a)
			for i := range a {
				if a[i] != b[i] {
					common = i
					break
				}
			}
			a, b = a[common:], b[common:]
			if len(a) > 64 {
				a = a[:64]
			}
			if len(b) > 64 {
				b = b[:64]
			}
			t.Errorf("after %d common bytes, difference \nread: %x\nwrote:%x", common, a, b)
		}
		t.Fatalf("expect read %d bytes but got %d", wroteBytes, readBytes)
	}
	if readHash.Sum32() != wroteHash.Sum32() {
		t.Fatalf("expect read hash 0x%08x but got 0x%08x", readHash.Sum32(), wroteHash.Sum32())
	}
}

func TestRingBuffer_BlockingBig(t *testing.T) {
	// Typical runtime is ~5-10s.
	defer timeout(60 * time.Second)()
	const debug = false

	var readBytes int
	var wroteBytes int
	readHash := crc32.NewIEEE()
	wroteHash := crc32.NewIEEE()
	var readBuf bytes.Buffer
	var wroteBuf bytes.Buffer
	read := io.Writer(readHash)
	wrote := io.Writer(wroteHash)
	if debug {
		read = io.MultiWriter(read, &readBuf)
		wrote = io.MultiWriter(wrote, &wroteBuf)
	}
	debugln := func(args ...any) {
		if debug {
			fmt.Println(args...)
		}
	}
	// Inject random reader/writer sleeps.
	const maxSleep = int(1 * time.Millisecond)
	doSleep := !testing.Short()
	rb := New(4 << 10).SetBlocking(true)

	// Reader
	var readErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer rb.CloseWithError(readErr)
		readRng := rand.New(rand.NewSource(1))
		buf := make([]byte, 64<<10)
		for {
			// Read
			n, err := rb.Read(buf[:readRng.Intn(len(buf))])
			readBytes += n
			read.Write(buf[:n])
			if err != nil {
				readErr = err
				break
			}
			debugln("READ 1\t", n, readBytes)

			// ReadByte
			b, err := rb.ReadByte()
			if err != nil {
				readErr = err
				break
			}
			readBytes++
			read.Write([]byte{b})
			debugln("READ 2\t", 1, readBytes)

			// TryRead
			n, err = rb.TryRead(buf[:readRng.Intn(len(buf))])
			readBytes += n
			read.Write(buf[:n])
			if err != nil && err != ErrAcquireLock && err != ErrIsEmpty {
				readErr = err
				break
			}
			debugln("READ 3\t", n, readBytes)
			if doSleep && readRng.Intn(20) == 0 {
				time.Sleep(time.Duration(readRng.Intn(maxSleep)))
			}
		}
	}()

	// Writer
	{
		writeRng := rand.New(rand.NewSource(2))
		buf := make([]byte, 64<<10)
		for range 500 {
			writeRng.Read(buf)
			// Write
			n, err := rb.Write(buf[:writeRng.Intn(len(buf))])
			if err != nil {
				t.Fatalf("write failed: %v", err)
			}
			wroteBytes += n
			wrote.Write(buf[:n])
			debugln("WRITE 1\t", n, wroteBytes)

			// WriteString
			n, err = rb.WriteString(string(buf[:writeRng.Intn(len(buf))]))
			if err != nil {
				t.Fatalf("write failed: %v", err)
			}
			wroteBytes += n
			wrote.Write(buf[:n])
			debugln("WRITE 2\t", writeRng.Intn(len(buf)), wroteBytes)

			// WriteByte
			err = rb.WriteByte(buf[0])
			if err != nil {
				t.Fatalf("write failed: %v", err)
			}
			wroteBytes++
			wrote.Write(buf[:1])
			debugln("WRITE 3\t", 1, wroteBytes)

			// TryWrite
			n, err = rb.TryWrite(buf[:writeRng.Intn(len(buf))])
			if err != nil && err != ErrAcquireLock && err != ErrTooMuchDataToWrite && err != ErrIsFull {
				t.Fatalf("write failed: %v", err)
			}
			wroteBytes += n
			wrote.Write(buf[:n])
			debugln("WRITE 4\t", n, wroteBytes)

			// TryWriteByte
			err = rb.TryWriteByte(buf[0])
			if err != nil && err != ErrAcquireLock && err != ErrTooMuchDataToWrite && err != ErrIsFull {
				t.Fatalf("write failed: %v", err)
			}
			if err == nil {
				wroteBytes++
				wrote.Write(buf[:1])
				debugln("WRITE 5\t", 1, wroteBytes)
			}
			if doSleep && writeRng.Intn(10) == 0 {
				time.Sleep(time.Duration(writeRng.Intn(maxSleep)))
			}
		}
		if err := rb.Flush(); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
		rb.CloseWriter()
	}
	wg.Wait()
	if !errors.Is(readErr, io.EOF) {
		t.Fatalf("expect io.EOF but got %v", readErr)
	}
	if readBytes != wroteBytes {
		a, b := readBuf.Bytes(), wroteBuf.Bytes()
		if debug && !bytes.Equal(a, b) {
			common := len(a)
			for i := range a {
				if a[i] != b[i] {
					t.Errorf("%x != %x", a[i], b[i])
					common = i
					break
				}
			}
			a, b = a[common:], b[common:]
			if len(a) > 64 {
				a = a[:64]
			}
			if len(b) > 64 {
				b = b[:64]
			}
			t.Errorf("after %d common bytes, difference \nread: %x\nwrote:%x", common, a, b)
		}
		t.Fatalf("expect read %d bytes but got %d", wroteBytes, readBytes)
	}
	if readHash.Sum32() != wroteHash.Sum32() {
		t.Fatalf("expect read hash 0x%08x but got 0x%08x", readHash.Sum32(), wroteHash.Sum32())
	}
}

func TestRingBuffer_ByteInterface(t *testing.T) {
	defer timeout(5 * time.Second)()
	rb := New(2)

	// write one
	err := rb.WriteByte('a')
	if err != nil {
		t.Fatalf("WriteByte failed: %v", err)
	}
	if rb.Length() != 1 {
		t.Fatalf("expect len 1 byte but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 1 {
		t.Fatalf("expect free 1 byte but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if !bytes.Equal(rb.Bytes(nil), []byte{'a'}) {
		t.Fatalf("expect a but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}
	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}

	// write to, isFull
	err = rb.WriteByte('b')
	if err != nil {
		t.Fatalf("WriteByte failed: %v", err)
	}
	if rb.Length() != 2 {
		t.Fatalf("expect len 2 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 byte but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if !bytes.Equal(rb.Bytes(nil), []byte{'a', 'b'}) {
		t.Fatalf("expect a but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}
	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if !rb.IsFull() {
		t.Fatalf("expect IsFull is true but got false")
	}

	// write
	err = rb.WriteByte('c')
	if err == nil {
		t.Fatalf("expect ErrIsFull but got nil")
	}
	if rb.Length() != 2 {
		t.Fatalf("expect len 2 bytes but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 0 {
		t.Fatalf("expect free 0 byte but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if !bytes.Equal(rb.Bytes(nil), []byte{'a', 'b'}) {
		t.Fatalf("expect a but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}
	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if !rb.IsFull() {
		t.Fatalf("expect IsFull is true but got false")
	}

	// read one
	b, err := rb.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte failed: %v", err)
	}
	if b != 'a' {
		t.Fatalf("expect a but got %c. r.w=%d, r.r=%d", b, rb.w, rb.r)
	}
	if rb.Length() != 1 {
		t.Fatalf("expect len 1 byte but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 1 {
		t.Fatalf("expect free 1 byte but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	if !bytes.Equal(rb.Bytes(nil), []byte{'b'}) {
		t.Fatalf("expect a but got %s. r.w=%d, r.r=%d", rb.Bytes(nil), rb.w, rb.r)
	}
	// check empty or full
	if rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is false but got true")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}

	// read two, empty
	b, err = rb.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte failed: %v", err)
	}
	if b != 'b' {
		t.Fatalf("expect b but got %c. r.w=%d, r.r=%d", b, rb.w, rb.r)
	}
	if rb.Length() != 0 {
		t.Fatalf("expect len 0 byte but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 2 {
		t.Fatalf("expect free 2 byte but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	// check empty or full
	if !rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is true but got false")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}

	// read three, error
	_, err = rb.ReadByte()
	if err == nil {
		t.Fatalf("expect ErrIsEmpty but got nil")
	}
	if rb.Length() != 0 {
		t.Fatalf("expect len 0 byte but got %d. r.w=%d, r.r=%d", rb.Length(), rb.w, rb.r)
	}
	if rb.Free() != 2 {
		t.Fatalf("expect free 2 byte but got %d. r.w=%d, r.r=%d", rb.Free(), rb.w, rb.r)
	}
	// check empty or full
	if !rb.IsEmpty() {
		t.Fatalf("expect IsEmpty is true but got false")
	}
	if rb.IsFull() {
		t.Fatalf("expect IsFull is false but got true")
	}
}

func TestRingBufferCloseError(t *testing.T) {
	type testError1 struct{ error }
	type testError2 struct{ error }

	rb := New(100)
	rb.CloseWithError(testError1{})
	if _, err := rb.Write(nil); err != (testError1{}) {
		t.Errorf("Write error: got %T, want testError1", err)
	}
	if _, err := rb.Write([]byte{1}); err != (testError1{}) {
		t.Errorf("Write error: got %T, want testError1", err)
	}
	if err := rb.WriteByte(0); err != (testError1{}) {
		t.Errorf("Write error: got %T, want testError1", err)
	}
	if _, err := rb.TryWrite(nil); err != (testError1{}) {
		t.Errorf("Write error: got %T, want testError1", err)
	}
	if _, err := rb.TryWrite([]byte{1}); err != (testError1{}) {
		t.Errorf("Write error: got %T, want testError1", err)
	}
	if err := rb.TryWriteByte(0); err != (testError1{}) {
		t.Errorf("Write error: got %T, want testError1", err)
	}
	if err := rb.Flush(); err != (testError1{}) {
		t.Errorf("Write error: got %T, want testError1", err)
	}

	rb.CloseWithError(testError2{})
	if _, err := rb.Write(nil); err != (testError1{}) {
		t.Errorf("Write error: got %T, want testError1", err)
	}

	rb.Reset()
	rb.CloseWithError(testError1{})
	if _, err := rb.Read(nil); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	if _, err := rb.Read([]byte{0}); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	if _, err := rb.ReadByte(); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	if _, err := rb.TryRead(nil); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	if _, err := rb.TryRead([]byte{0}); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	rb.CloseWithError(testError2{})
	if _, err := rb.Read(nil); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	if _, err := rb.Read([]byte{0}); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	if _, err := rb.ReadByte(); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	if _, err := rb.TryRead(nil); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
	if _, err := rb.TryRead([]byte{0}); err != (testError1{}) {
		t.Errorf("Read error: got %T, want testError1", err)
	}
}

func TestRingBufferCloseErrorUnblocks(t *testing.T) {
	const sz = 100
	rb := New(sz).SetBlocking(true)

	testCancel := func(fn func()) {
		t.Helper()
		defer timeout(5 * time.Second)()
		rb.Reset()
		done := make(chan struct{})
		go func() {
			defer close(done)
			time.Sleep(10 * time.Millisecond)
			fn()
		}()
		rb.CloseWithError(errors.New("test error"))
		<-done

		rb.Reset()
		done = make(chan struct{})
		go func() {
			defer close(done)
			fn()
		}()
		time.Sleep(10 * time.Millisecond)
		rb.CloseWithError(errors.New("test error"))
		<-done
	}
	testCancel(func() {
		rb.Write([]byte{sz + 5: 1})
	})
	testCancel(func() {
		rb.Write(make([]byte, sz))
		rb.WriteByte(0)
	})
	testCancel(func() {
		rb.Read([]byte{10: 1})
	})
	testCancel(func() {
		rb.ReadByte()
	})
	testCancel(func() {
		rb.Write(make([]byte, sz))
		rb.Flush()
	})
}

func TestWriteAfterWriterClose(t *testing.T) {
	rb := New(100).SetBlocking(true)

	done := make(chan error)
	go func() {
		defer close(done)
		_, err := rb.Write([]byte("hello"))
		if err != nil {
			t.Errorf("got error: %q; expected none", err)
		}
		rb.CloseWriter()
		_, err = rb.Write([]byte("world"))
		done <- err
		err = rb.WriteByte(0)
		done <- err
		_, err = rb.TryWrite([]byte("world"))
		done <- err
		err = rb.TryWriteByte(0)
		done <- err
	}()

	buf := make([]byte, 100)
	n, err := io.ReadFull(rb, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		t.Fatalf("got: %q; want: %q", err, io.ErrUnexpectedEOF)
	}
	for writeErr := range done {
		if writeErr != ErrWriteOnClosed {
			t.Errorf("got: %q; want: %q", writeErr, ErrWriteOnClosed)
		} else {
			t.Log("ok")
		}
	}
	result := string(buf[0:n])
	if result != "hello" {
		t.Errorf("got: %q; want: %q", result, "hello")
	}
}

func timeout(after time.Duration) (cancel func()) {
	c := time.After(after)
	cc := make(chan struct{})
	go func() {
		select {
		case <-cc:
			return
		case <-c:
			buf := make([]byte, 1<<20)
			stacklen := runtime.Stack(buf, true)
			fmt.Printf("=== Timeout, assuming deadlock ===\n*** goroutine dump...\n%s\n*** end\n", string(buf[:stacklen]))
			os.Exit(2)
		}
	}()
	return func() {
		close(cc)
	}
}
