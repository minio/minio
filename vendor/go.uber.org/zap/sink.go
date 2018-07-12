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
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"go.uber.org/zap/zapcore"
)

var (
	_sinkMutex     sync.RWMutex
	_sinkFactories map[string]func() (Sink, error)
)

func init() {
	resetSinkRegistry()
}

func resetSinkRegistry() {
	_sinkMutex.Lock()
	defer _sinkMutex.Unlock()
	_sinkFactories = map[string]func() (Sink, error){
		"stdout": func() (Sink, error) { return nopCloserSink{os.Stdout}, nil },
		"stderr": func() (Sink, error) { return nopCloserSink{os.Stderr}, nil },
	}
}

type errSinkNotFound struct {
	key string
}

func (e *errSinkNotFound) Error() string {
	return fmt.Sprintf("no sink found for %q", e.key)
}

// Sink defines the interface to write to and close logger destinations.
type Sink interface {
	zapcore.WriteSyncer
	io.Closer
}

// RegisterSink adds a Sink at the given key so it can be referenced
// in config OutputPaths.
func RegisterSink(key string, sinkFactory func() (Sink, error)) error {
	_sinkMutex.Lock()
	defer _sinkMutex.Unlock()
	if key == "" {
		return errors.New("sink key cannot be blank")
	}
	if _, ok := _sinkFactories[key]; ok {
		return fmt.Errorf("sink already registered for key %q", key)
	}
	_sinkFactories[key] = sinkFactory
	return nil
}

// newSink invokes the registered sink factory to create and return the
// sink for the given key. Returns errSinkNotFound if the key cannot be found.
func newSink(key string) (Sink, error) {
	_sinkMutex.RLock()
	defer _sinkMutex.RUnlock()
	sinkFactory, ok := _sinkFactories[key]
	if !ok {
		return nil, &errSinkNotFound{key}
	}
	return sinkFactory()
}

type nopCloserSink struct{ zapcore.WriteSyncer }

func (nopCloserSink) Close() error { return nil }
