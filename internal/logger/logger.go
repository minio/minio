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
	"encoding/hex"
	"errors"
	"fmt"
	"go/build"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/minio/highwayhash"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/madmin-go/v3/logger/log"
	"github.com/minio/minio/internal/color"
	xhttp "github.com/minio/minio/internal/http"
)

// HighwayHash key for logging in anonymous mode
var magicHighwayHash256Key = []byte("\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0")

// Enumerated level types
const (
	// Log types errors
	FatalKind   = madmin.LogKindFatal
	WarningKind = madmin.LogKindWarning
	ErrorKind   = madmin.LogKindError
	EventKind   = madmin.LogKindEvent
	InfoKind    = madmin.LogKindInfo
)

var (
	// DisableLog avoids printing error/event/info kind of logs
	DisableLog = false
	// Output allows configuring custom writer, defaults to os.Stderr
	Output io.Writer = os.Stderr
)

var trimStrings []string

// TimeFormat - logging time format.
const TimeFormat string = "15:04:05 MST 01/02/2006"

var matchingFuncNames = [...]string{
	"http.HandlerFunc.ServeHTTP",
	"cmd.serverMain",
	// add more here ..
}

// quietFlag: Hide startup messages if enabled
// jsonFlag: Display in JSON format, if enabled
var (
	quietFlag, jsonFlag, anonFlag bool
	// Custom function to format error
	// can be registered by RegisterError
	errorFmtFunc = func(introMsg string, err error, jsonFlag bool) string {
		return fmt.Sprintf("msg: %s\n err:%s", introMsg, err)
	}
)

// EnableQuiet - turns quiet option on.
func EnableQuiet() {
	color.TurnOff() // no colored outputs necessary in quiet mode.
	quietFlag = true
}

// EnableJSON - outputs logs in json format.
func EnableJSON() {
	color.TurnOff() // no colored outputs necessary in JSON mode.
	jsonFlag = true
	quietFlag = true
}

// EnableAnonymous - turns anonymous flag
// to avoid printing sensitive information.
func EnableAnonymous() {
	anonFlag = true
}

// IsJSON - returns true if jsonFlag is true
func IsJSON() bool {
	return jsonFlag
}

// IsQuiet - returns true if quietFlag is true
func IsQuiet() bool {
	return quietFlag
}

// RegisterError registers the specified rendering function. This latter
// will be called for a pretty rendering of fatal errors.
func RegisterError(f func(string, error, bool) string) {
	errorFmtFunc = f
}

// uniq swaps away duplicate elements in data, returning the size of the
// unique set. data is expected to be pre-sorted, and the resulting set in
// the range [0:size] will remain in sorted order. Uniq, following a
// sort.Sort call, can be used to prepare arbitrary inputs for use as sets.
func uniq(data sort.Interface) (size int) {
	p, l := 0, data.Len()
	if l <= 1 {
		return l
	}
	for i := 1; i < l; i++ {
		if !data.Less(p, i) {
			continue
		}
		p++
		if p < i {
			data.Swap(p, i)
		}
	}
	return p + 1
}

// Remove any duplicates and return unique entries.
func uniqueEntries(paths []string) []string {
	sort.Strings(paths)
	n := uniq(sort.StringSlice(paths))
	return paths[:n]
}

// Init sets the trimStrings to possible GOPATHs
// and GOROOT directories. Also append github.com/minio/minio
// This is done to clean up the filename, when stack trace is
// displayed when an error happens.
func Init(goPath string, goRoot string) {
	var goPathList []string
	var goRootList []string
	var defaultgoPathList []string
	var defaultgoRootList []string
	pathSeparator := ":"
	// Add all possible GOPATH paths into trimStrings
	// Split GOPATH depending on the OS type
	if runtime.GOOS == "windows" {
		pathSeparator = ";"
	}

	goPathList = strings.Split(goPath, pathSeparator)
	goRootList = strings.Split(goRoot, pathSeparator)
	defaultgoPathList = strings.Split(build.Default.GOPATH, pathSeparator)
	defaultgoRootList = strings.Split(build.Default.GOROOT, pathSeparator)

	// Add trim string "{GOROOT}/src/" into trimStrings
	trimStrings = []string{filepath.Join(runtime.GOROOT(), "src") + string(filepath.Separator)}

	// Add all possible path from GOPATH=path1:path2...:pathN
	// as "{path#}/src/" into trimStrings
	for _, goPathString := range goPathList {
		trimStrings = append(trimStrings, filepath.Join(goPathString, "src")+string(filepath.Separator))
	}

	for _, goRootString := range goRootList {
		trimStrings = append(trimStrings, filepath.Join(goRootString, "src")+string(filepath.Separator))
	}

	for _, defaultgoPathString := range defaultgoPathList {
		trimStrings = append(trimStrings, filepath.Join(defaultgoPathString, "src")+string(filepath.Separator))
	}

	for _, defaultgoRootString := range defaultgoRootList {
		trimStrings = append(trimStrings, filepath.Join(defaultgoRootString, "src")+string(filepath.Separator))
	}

	// Remove duplicate entries.
	trimStrings = uniqueEntries(trimStrings)

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

func getSource(level int) string {
	pc, file, lineNumber, ok := runtime.Caller(level)
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

	for ok && file != "" {
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

// HashString - return the highway hash of the passed string
func HashString(input string) string {
	hh, _ := highwayhash.New(magicHighwayHash256Key)
	hh.Write([]byte(input))
	return hex.EncodeToString(hh.Sum(nil))
}

// LogAlwaysIf prints a detailed error message during
// the execution of the server.
func LogAlwaysIf(ctx context.Context, subsystem string, err error, errKind ...any) {
	if err == nil {
		return
	}
	logIf(ctx, subsystem, err, errKind...)
}

// LogIf prints a detailed error message during
// the execution of the server, if it is not an
// ignored error.
func LogIf(ctx context.Context, subsystem string, err error, errKind ...any) {
	if logIgnoreError(err) {
		return
	}
	logIf(ctx, subsystem, err, errKind...)
}

// LogIfNot prints a detailed error message during
// the execution of the server, if it is not an ignored error (either internal or given).
func LogIfNot(ctx context.Context, subsystem string, err error, ignored ...error) {
	if logIgnoreError(err) {
		return
	}
	for _, ignore := range ignored {
		if errors.Is(err, ignore) {
			return
		}
	}
	logIf(ctx, subsystem, err)
}

func errToEntry(ctx context.Context, subsystem string, err error, errKind ...any) log.Entry {
	var l string
	if anonFlag {
		l = reflect.TypeOf(err).String()
	} else {
		l = fmt.Sprintf("%v (%T)", err, err)
	}
	return buildLogEntry(ctx, subsystem, l, getTrace(3), errKind...)
}

func logToEntry(ctx context.Context, subsystem, message string, errKind ...any) log.Entry {
	return buildLogEntry(ctx, subsystem, message, nil, errKind...)
}

func buildLogEntry(ctx context.Context, subsystem, message string, trace []string, errKind ...any) log.Entry {
	logKind := madmin.LogKindError
	if len(errKind) > 0 {
		if ek, ok := errKind[0].(madmin.LogKind); ok {
			logKind = ek
		}
	}

	req := GetReqInfo(ctx)
	if req == nil {
		req = &ReqInfo{
			API:       "SYSTEM",
			RequestID: fmt.Sprintf("%X", time.Now().UTC().UnixNano()),
		}
	}
	req.RLock()
	defer req.RUnlock()

	API := "SYSTEM"
	switch {
	case req.API != "":
		API = req.API
	case subsystem != "":
		API += "." + subsystem
	}

	// Copy tags. We hold read lock already.
	tags := make(map[string]any, len(req.tags))
	for _, entry := range req.tags {
		tags[entry.Key] = entry.Val
	}

	// Get the cause for the Error
	deploymentID := req.DeploymentID
	if req.DeploymentID == "" {
		deploymentID = xhttp.GlobalDeploymentID
	}

	objects := make([]log.ObjectVersion, 0, len(req.Objects))
	for _, ov := range req.Objects {
		objects = append(objects, log.ObjectVersion{
			ObjectName: ov.ObjectName,
			VersionID:  ov.VersionID,
		})
	}

	entry := log.Entry{
		DeploymentID: deploymentID,
		Level:        logKind,
		RemoteHost:   req.RemoteHost,
		Host:         req.Host,
		RequestID:    req.RequestID,
		UserAgent:    req.UserAgent,
		Time:         time.Now().UTC(),
		API: &log.API{
			Name: API,
			Args: &log.Args{
				Bucket:    req.BucketName,
				Object:    req.ObjectName,
				VersionID: req.VersionID,
				Objects:   objects,
			},
		},
	}

	if trace != nil {
		entry.Trace = &log.Trace{
			Message:   message,
			Source:    trace,
			Variables: tags,
		}
	} else {
		entry.Message = message
	}

	if anonFlag {
		entry.API.Args.Bucket = HashString(entry.API.Args.Bucket)
		entry.API.Args.Object = HashString(entry.API.Args.Object)
		entry.RemoteHost = HashString(entry.RemoteHost)
		if entry.Trace != nil {
			entry.Trace.Variables = make(map[string]any)
		}
	}

	return entry
}

// consoleLogIf prints a detailed error message during
// the execution of the server.
func consoleLogIf(ctx context.Context, subsystem string, err error, errKind ...any) {
	if DisableLog {
		return
	}
	if err == nil {
		return
	}
	if consoleTgt != nil {
		entry := errToEntry(ctx, subsystem, err, errKind...)
		consoleTgt.Send(ctx, entry)
	}
}

// logIf prints a detailed error message during
// the execution of the server.
func logIf(ctx context.Context, subsystem string, err error, errKind ...any) {
	if DisableLog {
		return
	}
	if err == nil {
		return
	}
	entry := errToEntry(ctx, subsystem, err, errKind...)
	sendLog(ctx, entry)
}

func sendLog(ctx context.Context, entry log.Entry) {
	systemTgts := SystemTargets()
	if len(systemTgts) == 0 {
		return
	}

	// Iterate over all logger targets to send the log entry
	for _, t := range systemTgts {
		if err := t.Send(ctx, entry); err != nil {
			if consoleTgt != nil { // Sending to the console never fails
				consoleTgt.Send(ctx, errToEntry(ctx, "logging", fmt.Errorf("unable to send log event to Logger target (%s): %v", t.String(), err), entry.Level))
			}
		}
	}
}

// Event sends a event log to  log targets
func Event(ctx context.Context, subsystem, msg string, args ...any) {
	if DisableLog {
		return
	}
	entry := logToEntry(ctx, subsystem, fmt.Sprintf(msg, args...), EventKind)
	sendLog(ctx, entry)
}

// ErrCritical is the value panic'd whenever CriticalIf is called.
var ErrCritical struct{}

// CriticalIf logs the provided error on the console. It fails the
// current go-routine by causing a `panic(ErrCritical)`.
func CriticalIf(ctx context.Context, err error, errKind ...any) {
	if err != nil {
		LogIf(ctx, "", err, errKind...)
		panic(ErrCritical)
	}
}

// FatalIf is similar to Fatal() but it ignores passed nil error
func FatalIf(err error, msg string, data ...any) {
	if err == nil {
		return
	}
	fatal(err, msg, data...)
}
