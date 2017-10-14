/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/minio/mc/pkg/console"
)

var log = NewLogger()

type loggers struct {
	sync.RWMutex
	Console ConsoleLogger `json:"console"`
	File    FileLogger    `json:"file"`
}

// Validate - Check whether loggers are valid or not.
func (l *loggers) Validate() (err error) {
	if l != nil {
		fileLogger := l.GetFile()
		if fileLogger.Enable && fileLogger.Filename == "" {
			err = errors.New("Missing filename for enabled file logger")
		}
	}

	return err
}

// SetFile set new file logger.
func (l *loggers) SetFile(flogger FileLogger) {
	l.Lock()
	defer l.Unlock()
	l.File = flogger
}

// GetFileLogger get current file logger.
func (l *loggers) GetFile() FileLogger {
	l.RLock()
	defer l.RUnlock()
	return l.File
}

// SetConsole set new console logger.
func (l *loggers) SetConsole(clogger ConsoleLogger) {
	l.Lock()
	defer l.Unlock()
	l.Console = clogger
}

// GetConsole get current console logger.
func (l *loggers) GetConsole() ConsoleLogger {
	l.RLock()
	defer l.RUnlock()
	return l.Console
}

// LogTarget - interface for log target.
type LogTarget interface {
	Fire(entry *logrus.Entry) error
	String() string
}

// BaseLogTarget - base log target.
type BaseLogTarget struct {
	Enable    bool `json:"enable"`
	formatter logrus.Formatter
}

// Logger - higher level logger.
type Logger struct {
	logger        *logrus.Logger
	consoleTarget ConsoleLogger
	targets       []LogTarget
	quiet         bool
}

// AddTarget - add logger to this hook.
func (log *Logger) AddTarget(logTarget LogTarget) {
	log.targets = append(log.targets, logTarget)
}

// SetConsoleTarget - sets console target to this hook.
func (log *Logger) SetConsoleTarget(consoleTarget ConsoleLogger) {
	log.consoleTarget = consoleTarget
}

// Fire - log entry handler to save logs.
func (log *Logger) Fire(entry *logrus.Entry) (err error) {
	if err = log.consoleTarget.Fire(entry); err != nil {
		log.Printf("Unable to log to console target. %s\n", err)
	}

	for _, logTarget := range log.targets {
		if err = logTarget.Fire(entry); err != nil {
			log.Printf("Unable to log to target %s. %s\n", logTarget, err)
		}
	}

	return err
}

// Levels - returns list of log levels support.
func (log *Logger) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

// EnableQuiet - sets quiet option.
func (log *Logger) EnableQuiet() {
	log.quiet = true
}

// Println - wrapper to console.Println() with quiet flag.
func (log *Logger) Println(args ...interface{}) {
	if !log.quiet {
		console.Println(args...)
	}
}

// Printf - wrapper to console.Printf() with quiet flag.
func (log *Logger) Printf(format string, args ...interface{}) {
	if !log.quiet {
		console.Printf(format, args...)
	}
}

// NewLogger - returns a new initialized logger.
func NewLogger() *Logger {
	logger := logrus.New()
	logger.Out = ioutil.Discard
	logger.Level = logrus.DebugLevel

	l := &Logger{
		logger:        logger,
		consoleTarget: NewConsoleLogger(),
	}

	// Adds a console logger.
	logger.Hooks.Add(l)

	return l
}

func getSource() string {
	var funcName string
	pc, filename, lineNum, ok := runtime.Caller(2)
	if ok {
		filename = path.Base(filename)
		funcName = strings.TrimPrefix(runtime.FuncForPC(pc).Name(), "github.com/minio/minio/cmd.")
	} else {
		filename = "<unknown>"
		lineNum = 0
	}

	return fmt.Sprintf("[%s:%d:%s()]", filename, lineNum, funcName)
}

func logIf(level logrus.Level, source string, err error, msg string, data ...interface{}) {
	isErrIgnored := func(err error) (ok bool) {
		err = errorCause(err)
		switch err.(type) {
		case BucketNotFound, BucketNotEmpty, BucketExists:
			ok = true
		case ObjectNotFound, ObjectExistsAsDirectory:
			ok = true
		case BucketPolicyNotFound, InvalidUploadID:
			ok = true
		}
		return ok
	}

	if err == nil || isErrIgnored(err) {
		return
	}

	fields := logrus.Fields{
		"source": source,
		"cause":  err.Error(),
	}

	if terr, ok := err.(*Error); ok {
		fields["stack"] = strings.Join(terr.Trace(), " ")
	}

	switch level {
	case logrus.PanicLevel:
		log.logger.WithFields(fields).Panicf(msg, data...)
	case logrus.FatalLevel:
		log.logger.WithFields(fields).Fatalf(msg, data...)
	case logrus.ErrorLevel:
		log.logger.WithFields(fields).Errorf(msg, data...)
	case logrus.WarnLevel:
		log.logger.WithFields(fields).Warnf(msg, data...)
	case logrus.InfoLevel:
		log.logger.WithFields(fields).Infof(msg, data...)
	default:
		log.logger.WithFields(fields).Debugf(msg, data...)
	}
}

func errorIf(err error, msg string, data ...interface{}) {
	logIf(logrus.ErrorLevel, getSource(), err, msg, data...)
}

func fatalIf(err error, msg string, data ...interface{}) {
	logIf(logrus.FatalLevel, getSource(), err, msg, data...)
}
