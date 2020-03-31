/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"

	"github.com/minio/minio/pkg/color"
)

// Err is a structure which contains all information
// to print a fatal error message in json or pretty mode
// Err implements error so we can use it anywhere
type Err struct {
	msg    string
	detail string
	action string
	hint   string
}

// Clone returns a new Err struct with the same information
func (u Err) Clone() Err {
	return Err{
		msg:    u.msg,
		detail: u.detail,
		action: u.action,
		hint:   u.hint,
	}
}

// Error returns the error message
func (u Err) Error() string {
	if u.detail == "" {
		if u.msg != "" {
			return u.msg
		}
		return "<nil>"
	}
	return u.detail
}

// Msg - Replace the current error's message
func (u Err) Msg(m string, args ...interface{}) Err {
	e := u.Clone()
	e.msg = fmt.Sprintf(m, args...)
	return e
}

// Hint - Replace the current error's message
func (u Err) Hint(m string, args ...interface{}) Err {
	e := u.Clone()
	e.hint = fmt.Sprintf(m, args...)
	return e
}

// ErrFn function wrapper
type ErrFn func(err error) Err

// Create a UI error generator, this is needed to simplify
// the update of the detailed error message in several places
// in MinIO code
func newErrFn(msg, action, hint string) ErrFn {
	return func(err error) Err {
		u := Err{
			msg:    msg,
			action: action,
			hint:   hint,
		}
		if err != nil {
			u.detail = err.Error()
		}
		return u
	}
}

// ErrorToErr inspects the passed error and transforms it
// to the appropriate UI error.
func ErrorToErr(err error) Err {
	if err == nil {
		return Err{}
	}

	// If this is already a Err, do nothing
	if e, ok := err.(Err); ok {
		return e
	}

	// Show a generic message for known golang errors
	if errors.Is(err, syscall.EADDRINUSE) {
		return ErrPortAlreadyInUse(err).Msg("Specified port is already in use")
	} else if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
		switch err.(type) {
		case *net.OpError:
			return ErrPortAccess(err).Msg("Insufficient permissions to use specified port")
		}
		return ErrNoPermissionsToAccessDirFiles(err).Msg("Insufficient permissions to access path")
	} else if errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrUnexpectedDataContent(err)
	} else {
		// Failed to identify what type of error this, return a simple UI error
		return Err{msg: err.Error()}
	}
}

// FmtError converts a fatal error message to a more clear error
// using some colors
func FmtError(introMsg string, err error, jsonFlag bool) string {
	renderedTxt := ""
	uiErr := ErrorToErr(err)
	// JSON print
	if jsonFlag {
		// Message text in json should be simple
		if uiErr.detail != "" {
			return uiErr.msg + ": " + uiErr.detail
		}
		return uiErr.msg
	}
	// Pretty print error message
	introMsg += ": "
	if uiErr.msg != "" {
		introMsg += color.Bold(uiErr.msg)
	} else {
		introMsg += color.Bold(err.Error())
	}
	renderedTxt += color.Red(introMsg) + "\n"
	// Add action message
	if uiErr.action != "" {
		renderedTxt += "> " + color.BgYellow(color.Black(uiErr.action)) + "\n"
	}
	// Add hint
	if uiErr.hint != "" {
		renderedTxt += color.Bold("HINT:") + "\n"
		renderedTxt += "  " + uiErr.hint
	}
	return renderedTxt
}
