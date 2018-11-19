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

package console

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/cmd/logger/message/log"
)

// Target implements loggerTarget to send log
// in plain or json format to the standard output.
type Target struct{}

// Send log message 'e' to console
func (c *Target) Send(e interface{}) error {
	entry, ok := e.(log.Entry)
	if !ok {
		return fmt.Errorf("Uexpected log entry structure %#v", e)
	}
	if logger.IsJSON() {
		logJSON, err := json.Marshal(&entry)
		if err != nil {
			return err
		}
		fmt.Println(string(logJSON))
		return nil
	}

	trace := make([]string, len(entry.Trace.Source))

	// Add a sequence number and formatting for each stack trace
	// No formatting is required for the first entry
	for i, element := range entry.Trace.Source {
		trace[i] = fmt.Sprintf("%8v: %s", i+1, element)
	}

	tagString := ""
	for key, value := range entry.Trace.Variables {
		if value != "" {
			if tagString != "" {
				tagString += ", "
			}
			tagString += key + "=" + value
		}
	}

	apiString := "API: " + entry.API.Name + "("
	if entry.API.Args != nil && entry.API.Args.Bucket != "" {
		apiString = apiString + "bucket=" + entry.API.Args.Bucket
	}
	if entry.API.Args != nil && entry.API.Args.Object != "" {
		apiString = apiString + ", object=" + entry.API.Args.Object
	}
	apiString += ")"
	timeString := "Time: " + time.Now().Format(logger.TimeFormat)

	var requestID string
	if entry.RequestID != "" {
		requestID = "\nRequestID: " + entry.RequestID
	}

	var remoteHost string
	if entry.RemoteHost != "" {
		remoteHost = "\nRemoteHost: " + entry.RemoteHost
	}

	var userAgent string
	if entry.UserAgent != "" {
		userAgent = "\nUserAgent: " + entry.UserAgent
	}

	if len(entry.Trace.Variables) > 0 {
		tagString = "\n       " + tagString
	}

	var msg = logger.ColorFgRed(logger.ColorBold(entry.Trace.Message))
	var output = fmt.Sprintf("\n%s\n%s%s%s%s\nError: %s%s\n%s",
		apiString, timeString, requestID, remoteHost, userAgent,
		msg, tagString, strings.Join(trace, "\n"))

	fmt.Println(output)
	return nil
}

// New initializes a new logger target
// which prints log directly in the standard
// output.
func New() *Target {
	return &Target{}
}
