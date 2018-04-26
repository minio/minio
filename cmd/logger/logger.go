/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
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

package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"go/build"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/fatih/color"
	c "github.com/minio/mc/pkg/console"
)

// global colors.
var (
	colorBold = color.New(color.Bold).SprintFunc()
	colorRed  = color.New(color.FgRed).SprintfFunc()
)

// Disable disables all logging, false by default. (used for "go test")
var Disable = false

var trimStrings []string

// Level type
type Level int8

// Enumerated level types
const (
	Information Level = iota + 1
	Error
	Fatal
)

const loggerTimeFormat string = "15:04:05 MST 01/02/2006"

var matchingFuncNames = [...]string{
	"http.HandlerFunc.ServeHTTP",
	"cmd.serverMain",
	"cmd.StartGateway",
	// add more here ..
}

func (level Level) String() string {
	var lvlStr string
	switch level {
	case Information:
		lvlStr = "INFO"
	case Error:
		lvlStr = "ERROR"
	case Fatal:
		lvlStr = "FATAL"
	}
	return lvlStr
}

// Console interface describes the methods that needs to be implemented to satisfy the interface requirements.
type Console interface {
	json(msg string, args ...interface{})
	quiet(msg string, args ...interface{})
	pretty(msg string, args ...interface{})
}

func consoleLog(console Console, msg string, args ...interface{}) {
	if Disable {
		return
	}
	if jsonFlag {
		console.json(msg, args...)
	} else if quiet {
		console.quiet(msg, args...)
	} else {
		console.pretty(msg, args...)
	}
}

type traceEntry struct {
	Message   string            `json:"message,omitempty"`
	Source    []string          `json:"source,omitempty"`
	Variables map[string]string `json:"variables,omitempty"`
}
type args struct {
	Bucket string `json:"bucket,omitempty"`
	Object string `json:"object,omitempty"`
}

type api struct {
	Name string `json:"name,omitempty"`
	Args *args  `json:"args,omitempty"`
}

type logEntry struct {
	Level      string      `json:"level"`
	Time       string      `json:"time"`
	API        *api        `json:"api,omitempty"`
	RemoteHost string      `json:"remotehost,omitempty"`
	RequestID  string      `json:"requestID,omitempty"`
	UserAgent  string      `json:"userAgent,omitempty"`
	Message    string      `json:"message,omitempty"`
	Trace      *traceEntry `json:"error,omitempty"`
}

// quiet: Hide startup messages if enabled
// jsonFlag: Display in JSON format, if enabled
var (
	quiet, jsonFlag bool
)

// EnableQuiet - turns quiet option on.
func EnableQuiet() {
	quiet = true
}

// EnableJSON - outputs logs in json format.
func EnableJSON() {
	jsonFlag = true
	quiet = true
}

// Init sets the trimStrings to possible GOPATHs
// and GOROOT directories. Also append github.com/minio/minio
// This is done to clean up the filename, when stack trace is
// displayed when an error happens.
func Init(goPath string) {
	var goPathList []string
	var defaultgoPathList []string
	// Add all possible GOPATH paths into trimStrings
	// Split GOPATH depending on the OS type
	if runtime.GOOS == "windows" {
		goPathList = strings.Split(goPath, ";")
		defaultgoPathList = strings.Split(build.Default.GOPATH, ";")
	} else {
		// All other types of OSs
		goPathList = strings.Split(goPath, ":")
		defaultgoPathList = strings.Split(build.Default.GOPATH, ":")

	}

	// Add trim string "{GOROOT}/src/" into trimStrings
	trimStrings = []string{filepath.Join(runtime.GOROOT(), "src") + string(filepath.Separator)}

	// Add all possible path from GOPATH=path1:path2...:pathN
	// as "{path#}/src/" into trimStrings
	for _, goPathString := range goPathList {
		trimStrings = append(trimStrings, filepath.Join(goPathString, "src")+string(filepath.Separator))
	}

	for _, defaultgoPathString := range defaultgoPathList {
		trimStrings = append(trimStrings, filepath.Join(defaultgoPathString, "src")+string(filepath.Separator))
	}

	// Add "github.com/minio/minio" as the last to cover
	// paths like "{GOROOT}/src/github.com/minio/minio"
	// and "{GOPATH}/src/github.com/minio/minio"
	trimStrings = append(trimStrings, filepath.Join("github.com", "minio", "minio")+string(filepath.Separator))
}

func trimTrace(f string) string {
	for _, trimString := range trimStrings {
		f = strings.TrimPrefix(filepath.ToSlash(f), filepath.ToSlash(trimString))
	}
	return filepath.FromSlash(f)
}

func getSource() string {
	pc, file, lineNumber, ok := runtime.Caller(5)
	if ok {
		// Clean up the common prefixes
		file = trimTrace(file)
		_, funcName := filepath.Split(runtime.FuncForPC(pc).Name())
		return fmt.Sprintf("%v:%v:%v()", file, lineNumber, funcName)
	}
	return ""
}

// getTrace method - creates and returns stack trace
func getTrace(traceLevel int) []string {
	var trace []string
	pc, file, lineNumber, ok := runtime.Caller(traceLevel)

	for ok {
		// Clean up the common prefixes
		file = trimTrace(file)
		// Get the function name
		_, funcName := filepath.Split(runtime.FuncForPC(pc).Name())
		// Skip duplicate traces that start with file name, "<autogenerated>"
		// and also skip traces with function name that starts with "runtime."
		if !strings.HasPrefix(file, "<autogenerated>") &&
			!strings.HasPrefix(funcName, "runtime.") {
			// Form and append a line of stack trace into a
			// collection, 'trace', to build full stack trace
			trace = append(trace, fmt.Sprintf("%v:%v:%v()", file, lineNumber, funcName))

			// Ignore trace logs beyond the following conditions
			for _, name := range matchingFuncNames {
				if funcName == name {
					return trace
				}
			}
		}
		traceLevel++
		// Read stack trace information from PC
		pc, file, lineNumber, ok = runtime.Caller(traceLevel)
	}
	return trace
}

// LogIf :
func LogIf(ctx context.Context, err error) {
	if Disable {
		return
	}

	if err == nil {
		return
	}

	req := GetReqInfo(ctx)

	if req == nil {
		req = &ReqInfo{API: "SYSTEM"}
	}

	API := "SYSTEM"
	if req.API != "" {
		API = req.API
	}

	tags := make(map[string]string)
	for _, entry := range req.GetTags() {
		tags[entry.Key] = entry.Val
	}

	// Get the cause for the Error
	message := err.Error()
	// Get full stack trace
	trace := getTrace(2)
	// Output the formatted log message at console
	var output string
	if jsonFlag {
		logJSON, err := json.Marshal(&logEntry{
			Level:      Error.String(),
			RemoteHost: req.RemoteHost,
			RequestID:  req.RequestID,
			UserAgent:  req.UserAgent,
			Time:       time.Now().UTC().Format(time.RFC3339Nano),
			API:        &api{Name: API, Args: &args{Bucket: req.BucketName, Object: req.ObjectName}},
			Trace:      &traceEntry{Message: message, Source: trace, Variables: tags},
		})
		if err != nil {
			panic(err)
		}
		output = string(logJSON)
	} else {
		// Add a sequence number and formatting for each stack trace
		// No formatting is required for the first entry
		for i, element := range trace {
			trace[i] = fmt.Sprintf("%8v: %s", i+1, element)
		}

		tagString := ""
		for key, value := range tags {
			if value != "" {
				if tagString != "" {
					tagString += ", "
				}
				tagString += key + "=" + value
			}
		}

		apiString := "API: " + API + "("
		if req.BucketName != "" {
			apiString = apiString + "bucket=" + req.BucketName
		}
		if req.ObjectName != "" {
			apiString = apiString + ", object=" + req.ObjectName
		}
		apiString += ")"
		timeString := "Time: " + time.Now().Format(loggerTimeFormat)

		var requestID string
		if req.RequestID != "" {
			requestID = "\nRequestID: " + req.RequestID
		}

		var remoteHost string
		if req.RemoteHost != "" {
			remoteHost = "\nRemoteHost: " + req.RemoteHost
		}

		var userAgent string
		if req.UserAgent != "" {
			userAgent = "\nUserAgent: " + req.UserAgent
		}

		if len(tags) > 0 {
			tagString = "\n       " + tagString
		}

		output = fmt.Sprintf("\n%s\n%s%s%s%s\nError: %s%s\n%s",
			apiString, timeString, requestID, remoteHost, userAgent,
			colorRed(colorBold(message)), tagString, strings.Join(trace, "\n"))
	}
	fmt.Println(output)
}

// CriticalIf :
// Like LogIf with exit
// It'll be called for fatal error conditions during run-time
func CriticalIf(ctx context.Context, err error) {
	if err != nil {
		LogIf(ctx, err)
		os.Exit(1)
	}
}

// FatalIf :
// Just fatal error message, no stack trace
// It'll be called for input validation failures
func FatalIf(err error, msg string, data ...interface{}) {
	if err != nil {
		if msg != "" {
			consoleLog(fatalMessage, msg, data...)
		} else {
			consoleLog(fatalMessage, err.Error())
		}
	}
}

var fatalMessage fatalMsg

type fatalMsg struct {
}

func (f fatalMsg) json(msg string, args ...interface{}) {
	logJSON, err := json.Marshal(&logEntry{
		Level: Fatal.String(),
		Time:  time.Now().UTC().Format(time.RFC3339Nano),
		Trace: &traceEntry{Message: fmt.Sprintf(msg, args...), Source: []string{getSource()}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(logJSON))
	os.Exit(1)

}

func (f fatalMsg) quiet(msg string, args ...interface{}) {
	f.pretty(msg, args...)
}

func (f fatalMsg) pretty(msg string, args ...interface{}) {
	errMsg := fmt.Sprintf(msg, args...)
	fmt.Println(colorRed(colorBold("Error: " + errMsg)))
	os.Exit(1)
}

var info infoMsg

type infoMsg struct {
}

func (i infoMsg) json(msg string, args ...interface{}) {
	logJSON, err := json.Marshal(&logEntry{
		Level:   Information.String(),
		Message: fmt.Sprintf(msg, args...),
		Time:    time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(logJSON))
}

func (i infoMsg) quiet(msg string, args ...interface{}) {
	i.pretty(msg, args...)
}

func (i infoMsg) pretty(msg string, args ...interface{}) {
	c.Printf(msg, args...)
}

// Info :
func Info(msg string, data ...interface{}) {
	consoleLog(info, msg+"\n", data...)
}

var startupMessage startUpMsg

type startUpMsg struct {
}

func (s startUpMsg) json(msg string, args ...interface{}) {
}

func (s startUpMsg) quiet(msg string, args ...interface{}) {
}

func (s startUpMsg) pretty(msg string, args ...interface{}) {
	c.Printf(msg, args...)
}

// StartupMessage :
func StartupMessage(msg string, data ...interface{}) {
	consoleLog(startupMessage, msg+"\n", data...)
}
