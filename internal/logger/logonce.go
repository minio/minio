// Copyright (c) 2015-2021 MinIO, Inc.
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

package logger

import (
	"context"
	"errors"
	"sync"
	"time"
)

// LogOnce provides the function type for logger.LogOnceIf() function
type LogOnce func(ctx context.Context, err error, id string, errKind ...any)

type onceErr struct {
	Err   error
	Count int
}

// Holds a map of recently logged errors.
type logOnceType struct {
	IDMap map[string]onceErr
	sync.Mutex
}

func (l *logOnceType) logOnceConsoleIf(ctx context.Context, subsystem string, err error, id string, errKind ...any) {
	if err == nil {
		return
	}

	nerr := unwrapErrs(err)
	l.Lock()
	shouldLog := true
	prev, ok := l.IDMap[id]
	if !ok {
		l.IDMap[id] = onceErr{
			Err:   nerr,
			Count: 1,
		}
	} else if prev.Err.Error() == nerr.Error() {
		// if errors are equal do not log.
		prev.Count++
		l.IDMap[id] = prev
		shouldLog = false
	}
	l.Unlock()

	if shouldLog {
		consoleLogIf(ctx, subsystem, err, errKind...)
	}
}

const unwrapErrsDepth = 3

// unwrapErrs upto the point where errors.Unwrap(err) returns nil
func unwrapErrs(err error) (leafErr error) {
	uerr := errors.Unwrap(err)
	depth := 1
	for uerr != nil {
		// Save the current `uerr`
		leafErr = uerr
		// continue to look for leaf errors underneath
		uerr = errors.Unwrap(leafErr)
		depth++
		if depth == unwrapErrsDepth {
			// If we have reached enough depth we
			// do not further recurse down, this
			// is done to avoid any unnecessary
			// latencies this might bring.
			break
		}
	}
	if uerr == nil {
		leafErr = err
	}
	return leafErr
}

// One log message per error.
func (l *logOnceType) logOnceIf(ctx context.Context, subsystem string, err error, id string, errKind ...any) {
	if err == nil {
		return
	}

	nerr := unwrapErrs(err)
	l.Lock()
	shouldLog := true
	prev, ok := l.IDMap[id]
	if !ok {
		l.IDMap[id] = onceErr{
			Err:   nerr,
			Count: 1,
		}
	} else if prev.Err.Error() == nerr.Error() {
		// if errors are equal do not log.
		prev.Count++
		l.IDMap[id] = prev
		shouldLog = false
	}
	l.Unlock()

	if shouldLog {
		logIf(ctx, subsystem, err, errKind...)
	}
}

// Cleanup the map every one hour so that the log message is printed again for the user to notice.
func (l *logOnceType) cleanupRoutine() {
	for {
		time.Sleep(time.Hour)

		l.Lock()
		l.IDMap = make(map[string]onceErr)
		l.Unlock()
	}
}

// Returns logOnceType
func newLogOnceType() *logOnceType {
	l := &logOnceType{IDMap: make(map[string]onceErr)}
	go l.cleanupRoutine()
	return l
}

var logOnce = newLogOnceType()

// LogOnceIf - Logs notification errors - once per error.
// id is a unique identifier for related log messages, refer to cmd/notification.go
// on how it is used.
func LogOnceIf(ctx context.Context, subsystem string, err error, id string, errKind ...any) {
	if logIgnoreError(err) {
		return
	}
	logOnce.logOnceIf(ctx, subsystem, err, id, errKind...)
}

// LogOnceConsoleIf - similar to LogOnceIf but exclusively only logs to console target.
func LogOnceConsoleIf(ctx context.Context, subsystem string, err error, id string, errKind ...any) {
	if logIgnoreError(err) {
		return
	}
	logOnce.logOnceConsoleIf(ctx, subsystem, err, id, errKind...)
}
