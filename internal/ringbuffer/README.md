# ringbuffer

[![License](https://img.shields.io/:license-MIT-blue.svg)](https://opensource.org/licenses/MIT) [![GoDoc](https://godoc.org/github.com/smallnest/ringbuffer?status.png)](http://godoc.org/github.com/smallnest/ringbuffer)  [![Go Report Card](https://goreportcard.com/badge/github.com/smallnest/ringbuffer)](https://goreportcard.com/report/github.com/smallnest/ringbuffer) [![coveralls](https://coveralls.io/repos/smallnest/ringbuffer/badge.svg?branch=master&service=github)](https://coveralls.io/github/smallnest/ringbuffer?branch=master) 

A circular buffer (ring buffer) in Go, implemented io.ReaderWriter interface

[![wikipedia](Circular_Buffer_Animation.gif)](https://github.com/smallnest/ringbuffer)

# Usage

```go
package main

import (
	"fmt"

	"github.com/smallnest/ringbuffer"
)

func main() {
	rb := ringbuffer.New(1024)

	// write
	rb.Write([]byte("abcd"))
	fmt.Println(rb.Length())
	fmt.Println(rb.Free())

	// read
	buf := make([]byte, 4)
	rb.Read(buf)
	fmt.Println(string(buf))
}
```

It is possible to use an existing buffer with by replacing `New` with `NewBuffer`.


# Blocking vs Non-blocking

The default behavior of the ring buffer is non-blocking, 
meaning that reads and writes will return immediately with an error if the operation cannot be completed.
If you want to block when reading or writing, you must enable it:

```go
	rb := ringbuffer.New(1024).SetBlocking(true)
```

Enabling blocking will cause the ring buffer to behave like a buffered [io.Pipe](https://pkg.go.dev/io#Pipe).

Regular Reads will block until data is available, but not wait for a full buffer. 
Writes will block until there is space available and writes bigger than the buffer will wait for reads to make space.

`TryRead` and `TryWrite` are still available for non-blocking reads and writes.

To signify the end of the stream, close the ring buffer from the writer side with `rb.CloseWriter()`

Either side can use `rb.CloseWithError(err error)` to signal an error and close the ring buffer. 
Any reads or writes will return the error on next call.

In blocking mode errors are stateful and the same error will be returned until `rb.Reset()` is called.