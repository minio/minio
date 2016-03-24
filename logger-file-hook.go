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

package main

import (
	"fmt"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/minio/minio/pkg/probe"
)

type fileLogger struct {
	Enable   bool   `json:"enable"`
	Filename string `json:"fileName"`
	Level    string `json:"level"`
}

type localFile struct {
	*os.File
}

func enableFileLogger(filename string) {
	file, e := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	fatalIf(probe.NewError(e), "Unable to open log file.", nil)

	// Add a local file hook.
	log.Hooks.Add(&localFile{file})
	// Set default JSON formatter.
	log.Formatter = new(logrus.JSONFormatter)
	log.Level = logrus.InfoLevel // Minimum log level.
}

// Fire fires the file logger hook and logs to the file.
func (l *localFile) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		return fmt.Errorf("Unable to read entry, %v", err)
	}
	l.File.Write([]byte(line + "\n"))
	l.File.Sync()
	return nil
}

// Levels -
func (l *localFile) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
