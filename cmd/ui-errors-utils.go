/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package cmd

import (
	"fmt"
	"io"
	"net"
	"os"
)

// uiErr is a structure which contains all information
// to print a fatal error message in json or pretty mode
// uiErr implements error so we can use it anywhere
type uiErr struct {
	msg    string
	detail string
	action string
	help   string
}

// Return the error message
func (u uiErr) Error() string {
	if u.detail == "" {
		return u.msg
	}
	return u.detail
}

// Replace the current error's message
func (u uiErr) Msg(m string, args ...interface{}) uiErr {
	return uiErr{
		msg:    fmt.Sprintf(m, args...),
		detail: u.detail,
		action: u.action,
		help:   u.help,
	}
}

type uiErrFn func(err error) uiErr

// Create a UI error generator, this is needed to simplify
// the update of the detailed error message in several places
// in Minio code
func newUIErrFn(msg, action, help string) uiErrFn {
	return func(err error) uiErr {
		u := uiErr{
			msg:    msg,
			action: action,
			help:   help,
		}
		if err != nil {
			u.detail = err.Error()
		}
		return u
	}
}

// errorToUIError inspects the passed error and transforms it
// to the appropriate UI error.
func errorToUIErr(err error) uiErr {
	// If this is already a uiErr, do nothing
	if e, ok := err.(uiErr); ok {
		return e
	}

	// Show a generic message for known golang errors
	switch e := err.(type) {
	case *net.OpError:
		if e.Op == "listen" {
			return uiErrPortAlreadyInUse(e).Msg("Port " + e.Addr.String() + " is already in use")

		}
	case *os.PathError:
		if os.IsPermission(e) {
			return uiErrNoPermissionsToAccessDirFiles(e).Msg("Insufficient permissions to access path, `" + e.Path + "`")
		}
	}

	switch err {
	case io.ErrUnexpectedEOF:
		return uiErrUnexpectedDataContent(err)
	default:
		// Failed to identify what type of error this, return a simple UI error
		return uiErr{msg: err.Error()}
	}

}

// fmtError() converts a fatal error message to a more clear error
// using some colors
func fmtError(introMsg string, err error, jsonFlag bool) string {
	renderedTxt := ""
	uiErr := errorToUIErr(err)
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
		introMsg += colorBold(uiErr.msg + ".")
	} else {
		introMsg += colorBold(err.Error() + ".")
	}
	renderedTxt += colorRed(introMsg) + "\n"
	// Add action message
	if uiErr.action != "" {
		renderedTxt += "> " + colorBgYellow(colorBlack(uiErr.action+".")) + "\n"
	}
	// Add help
	if uiErr.help != "" {
		renderedTxt += colorBold("HELP:") + "\n"
		renderedTxt += "  " + uiErr.help
	}
	return renderedTxt
}
