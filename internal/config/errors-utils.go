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

package config

import (
	"errors"
	"fmt"
	"net"
	"syscall"

	"github.com/minio/minio/internal/color"
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
func (u Err) Msg(m string) Err {
	e := u.Clone()
	e.msg = m
	return e
}

// Msgf - Replace the current error's message
func (u Err) Msgf(m string, args ...any) Err {
	e := u.Clone()
	if len(args) == 0 {
		e.msg = m
	} else {
		e.msg = fmt.Sprintf(m, args...)
	}
	return e
}

// Hint - Replace the current error's message
func (u Err) Hint(m string, args ...any) Err {
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
		if netErr, ok := err.(*net.OpError); ok {
			return ErrPortAccess(netErr).Msg("Insufficient permissions to use specified port")
		}
	}

	// Failed to identify what type of error this, return a simple UI error
	return Err{msg: err.Error()}
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
