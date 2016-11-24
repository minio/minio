/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
)

type fields map[string]interface{}

var log = struct {
	loggers []*logrus.Logger // All registered loggers.
	mu      sync.Mutex
}{}

// logger carries logging configuration for various supported loggers.
// Currently supported loggers are
//
//   - console [default]
//   - file
type logger struct {
	Console consoleLogger `json:"console"`
	File    fileLogger    `json:"file"`
	// Add new loggers here.
}

// Get file, line, function name of the caller.
func callerSource() string {
	pc, file, line, success := runtime.Caller(2)
	if !success {
		file = "<unknown>"
		line = 0
	}
	file = path.Base(file)
	name := runtime.FuncForPC(pc).Name()
	name = strings.TrimPrefix(name, "github.com/minio/minio/cmd.")
	return fmt.Sprintf("[%s:%d:%s()]", file, line, name)
}

// errorIf synonymous with fatalIf but doesn't exit on error != nil
func errorIf(err error, msg string, data ...interface{}) {
	if err == nil || !isErrLogged(err) {
		return
	}
	source := callerSource()
	fields := logrus.Fields{
		"source": source,
		"cause":  err.Error(),
	}
	if e, ok := err.(*Error); ok {
		fields["stack"] = strings.Join(e.Trace(), " ")
	}

	for _, log := range log.loggers {
		log.WithFields(fields).Errorf(msg, data...)
	}
}

// fatalIf wrapper function which takes error and prints jsonic error messages.
func fatalIf(err error, msg string, data ...interface{}) {
	if err == nil || !isErrLogged(err) {
		return
	}
	source := callerSource()
	fields := logrus.Fields{
		"source": source,
		"cause":  err.Error(),
	}
	if e, ok := err.(*Error); ok {
		fields["stack"] = strings.Join(e.Trace(), " ")
	}
	for _, log := range log.loggers {
		log.WithFields(fields).Fatalf(msg, data...)
	}
}

// returns false if error is not supposed to be logged.
func isErrLogged(err error) (ok bool) {
	ok = true
	err = errorCause(err)
	switch err.(type) {
	case BucketNotFound, BucketNotEmpty, BucketExists:
		ok = false
	case ObjectNotFound, ObjectExistsAsDirectory:
		ok = false
	case BucketPolicyNotFound, InvalidUploadID:
		ok = false
	case BadDigest:
		ok = false
	}
	return ok
}
