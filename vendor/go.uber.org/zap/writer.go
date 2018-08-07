// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zap

import (
	"io"
	"io/ioutil"
	"os"

	"go.uber.org/zap/zapcore"

	"go.uber.org/multierr"
)

// Open is a high-level wrapper that takes a variadic number of paths, opens or
// creates each of the specified files, and combines them into a locked
// WriteSyncer. It also returns any error encountered and a function to close
// any opened files.
//
// Passing no paths returns a no-op WriteSyncer. The special paths "stdout" and
// "stderr" are interpreted as os.Stdout and os.Stderr, respectively.
func Open(paths ...string) (zapcore.WriteSyncer, func(), error) {
	writers, close, err := open(paths)
	if err != nil {
		return nil, nil, err
	}

	writer := CombineWriteSyncers(writers...)
	return writer, close, nil
}

func open(paths []string) ([]zapcore.WriteSyncer, func(), error) {
	var openErr error
	writers := make([]zapcore.WriteSyncer, 0, len(paths))
	closers := make([]io.Closer, 0, len(paths))
	close := func() {
		for _, c := range closers {
			c.Close()
		}
	}
	for _, path := range paths {
		sink, err := newSink(path)
		if err == nil {
			// Using a registered sink constructor.
			writers = append(writers, sink)
			closers = append(closers, sink)
			continue
		}
		if _, ok := err.(*errSinkNotFound); ok {
			// No named sink constructor, use key as path to log file.
			f, e := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
			openErr = multierr.Append(openErr, e)
			if e == nil {
				writers = append(writers, f)
				closers = append(closers, f)
			}
			continue
		}
		// Sink constructor failed.
		openErr = multierr.Append(openErr, err)
	}

	if openErr != nil {
		close()
		return writers, nil, openErr
	}

	return writers, close, nil
}

// CombineWriteSyncers is a utility that combines multiple WriteSyncers into a
// single, locked WriteSyncer. If no inputs are supplied, it returns a no-op
// WriteSyncer.
//
// It's provided purely as a convenience; the result is no different from
// using zapcore.NewMultiWriteSyncer and zapcore.Lock individually.
func CombineWriteSyncers(writers ...zapcore.WriteSyncer) zapcore.WriteSyncer {
	if len(writers) == 0 {
		return zapcore.AddSync(ioutil.Discard)
	}
	return zapcore.Lock(zapcore.NewMultiWriteSyncer(writers...))
}
