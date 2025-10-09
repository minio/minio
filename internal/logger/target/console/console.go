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

package console

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/minio/madmin-go/v3/logger/log"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/logger"
)

// Target implements loggerTarget to send log
// in plain or json format to the standard output.
type Target struct {
	output io.Writer
}

// Validate - validate if the tty can be written to
func (c *Target) Validate() error {
	return nil
}

// Endpoint returns the backend endpoint
func (c *Target) Endpoint() string {
	return ""
}

func (c *Target) String() string {
	return "console"
}

// Send log message 'e' to console
func (c *Target) Send(e any) error {
	entry, ok := e.(log.Entry)
	if !ok {
		return fmt.Errorf("Uexpected log entry structure %#v", e)
	}
	if logger.IsJSON() {
		logJSON, err := json.Marshal(&entry)
		if err != nil {
			return err
		}
		fmt.Fprintln(c.output, string(logJSON))
		return nil
	}

	if entry.Level == logger.EventKind {
		fmt.Fprintln(c.output, entry.Message)
		return nil
	}

	traceLength := len(entry.Trace.Source)
	trace := make([]string, traceLength)

	// Add a sequence number and formatting for each stack trace
	// No formatting is required for the first entry
	for i, element := range entry.Trace.Source {
		trace[i] = fmt.Sprintf("%8v: %s", traceLength-i, element)
	}

	tagString := ""
	for key, value := range entry.Trace.Variables {
		if value != "" {
			if tagString != "" {
				tagString += ", "
			}
			tagString += fmt.Sprintf("%s=%#v", key, value)
		}
	}

	var apiString string
	if entry.API != nil {
		apiString = "API: " + entry.API.Name
		if entry.API.Args != nil {
			args := ""
			if entry.API.Args.Bucket != "" {
				args = args + "bucket=" + entry.API.Args.Bucket
			}
			if entry.API.Args.Object != "" {
				args = args + ", object=" + entry.API.Args.Object
			}
			if entry.API.Args.VersionID != "" {
				args = args + ", versionId=" + entry.API.Args.VersionID
			}
			if len(entry.API.Args.Objects) > 0 {
				args = args + ", multiObject=true, numberOfObjects=" + strconv.Itoa(len(entry.API.Args.Objects))
			}
			if len(args) > 0 {
				apiString += "(" + args + ")"
			}
		}
	} else {
		apiString = "INTERNAL"
	}
	timeString := "Time: " + entry.Time.Format(logger.TimeFormat)

	var deploymentID string
	if entry.DeploymentID != "" {
		deploymentID = "\nDeploymentID: " + entry.DeploymentID
	}

	var requestID string
	if entry.RequestID != "" {
		requestID = "\nRequestID: " + entry.RequestID
	}

	var remoteHost string
	if entry.RemoteHost != "" {
		remoteHost = "\nRemoteHost: " + entry.RemoteHost
	}

	var host string
	if entry.Host != "" {
		host = "\nHost: " + entry.Host
	}

	var userAgent string
	if entry.UserAgent != "" {
		userAgent = "\nUserAgent: " + entry.UserAgent
	}

	if len(entry.Trace.Variables) > 0 {
		tagString = "\n       " + tagString
	}

	msg := color.RedBold(entry.Trace.Message)
	output := fmt.Sprintf("\n%s\n%s%s%s%s%s%s\nError: %s%s\n%s",
		apiString, timeString, deploymentID, requestID, remoteHost, host, userAgent,
		msg, tagString, strings.Join(trace, "\n"))

	fmt.Fprintln(c.output, output)
	return nil
}

// New initializes a new logger target
// which prints log directly in the standard
// output.
func New(w io.Writer) *Target {
	return &Target{output: w}
}
