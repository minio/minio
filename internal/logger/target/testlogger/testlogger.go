// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// Package testlogger contains an autoregistering logger that can be used to capture logging events
// for individual tests.
// This package should only be included by test files.
// To enable logging for a test, use:
//
//	func TestSomething(t *testing.T) {
//		defer testlogger.T.SetLogTB(t)()
//
// This cannot be used for parallel tests.
package testlogger

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/minio/madmin-go/v3/logger/log"
	"github.com/minio/minio/internal/logger"
	types "github.com/minio/minio/internal/logger/target/loggertypes"
)

const (
	logMessage = iota
	errorMessage
	fatalMessage
)

// T is the test logger.
var T = &testLogger{}

func init() {
	logger.AddSystemTarget(context.Background(), T)
}

type testLogger struct {
	current atomic.Pointer[testing.TB]
	action  atomic.Int32
}

// SetLogTB will set the logger to output to tb.
// Call the returned function to disable logging.
func (t *testLogger) SetLogTB(tb testing.TB) func() {
	return t.setTB(tb, logMessage)
}

// SetErrorTB will set the logger to output to tb.Error.
// Call the returned function to disable logging.
func (t *testLogger) SetErrorTB(tb testing.TB) func() {
	return t.setTB(tb, errorMessage)
}

// SetFatalTB will set the logger to output to tb.Panic.
// Call the returned function to disable logging.
func (t *testLogger) SetFatalTB(tb testing.TB) func() {
	return t.setTB(tb, fatalMessage)
}

func (t *testLogger) setTB(tb testing.TB, action int32) func() {
	old := t.action.Swap(action)
	t.current.Store(&tb)
	return func() {
		t.current.Store(nil)
		t.action.Store(old)
	}
}

func (t *testLogger) String() string {
	tb := t.current.Load()
	if tb != nil {
		tbb := *tb
		return tbb.Name()
	}
	return ""
}

func (t *testLogger) Endpoint() string {
	return ""
}

func (t *testLogger) Stats() types.TargetStats {
	return types.TargetStats{}
}

func (t *testLogger) Init(ctx context.Context) error {
	return nil
}

func (t *testLogger) IsOnline(ctx context.Context) bool {
	return t.current.Load() != nil
}

func (t *testLogger) Cancel() {
	t.current.Store(nil)
}

func (t *testLogger) Send(ctx context.Context, entry any) error {
	tb := t.current.Load()
	var logf func(format string, args ...any)
	if tb != nil {
		tbb := *tb
		tbb.Helper()
		switch t.action.Load() {
		case errorMessage:
			logf = tbb.Errorf
		case fatalMessage:
			logf = tbb.Fatalf
		default:
			logf = tbb.Logf
		}
	} else {
		switch t.action.Load() {
		case errorMessage:
			logf = func(format string, args ...any) {
				fmt.Fprintf(os.Stderr, format+"\n", args...)
			}
		case fatalMessage:
			logf = func(format string, args ...any) {
				fmt.Fprintf(os.Stderr, format+"\n", args...)
			}
			defer os.Exit(1)
		default:
			logf = func(format string, args ...any) {
				fmt.Fprintf(os.Stdout, format+"\n", args...)
			}
		}
	}

	switch v := entry.(type) {
	case log.Entry:
		if v.Trace == nil {
			logf("%s: %s", v.Level, v.Message)
		} else {
			msg := fmt.Sprintf("%s: %+v", v.Level, v.Trace.Message)
			for i, m := range v.Trace.Source {
				if i == 0 && strings.Contains(m, "logger.go:") {
					continue
				}
				msg += fmt.Sprintf("\n%s", m)
			}
			logf("%s", msg)
		}
	default:
		logf("%+v (%T)", v, v)
	}
	return nil
}

func (t *testLogger) Type() types.TargetType {
	return types.TargetConsole
}
