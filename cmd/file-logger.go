/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"os"

	"github.com/sirupsen/logrus"
)

// FileLogger - file logger which logs to a file.
type FileLogger struct {
	BaseLogTarget
	Filename string `json:"filename"`
	file     *os.File
}

// Fire - log entry handler.
func (logger FileLogger) Fire(entry *logrus.Entry) (err error) {
	if !logger.Enable {
		return nil
	}

	msgBytes, err := logger.formatter.Format(entry)
	if err != nil {
		return err
	}

	if _, err = logger.file.Write(msgBytes); err != nil {
		return err
	}

	err = logger.file.Sync()
	return err
}

// String - represents ConsoleLogger as string.
func (logger FileLogger) String() string {
	enableStr := "disabled"
	if logger.Enable {
		enableStr = "enabled"
	}

	return fmt.Sprintf("file:%s:%s", enableStr, logger.Filename)
}

// NewFileLogger - creates new file logger object.
func NewFileLogger(filename string) (logger FileLogger) {
	logger.Enable = true
	logger.formatter = new(logrus.JSONFormatter)
	logger.Filename = filename

	return logger
}

// InitFileLogger - initializes file logger.
func InitFileLogger(logger *FileLogger) (err error) {
	if !logger.Enable {
		return err
	}

	if logger.formatter == nil {
		logger.formatter = new(logrus.JSONFormatter)
	}

	if logger.file == nil {
		logger.file, err = os.OpenFile(logger.Filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0664)
	}

	return err
}
