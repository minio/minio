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

// ConsoleLogger - console logger which logs into stderr.
type ConsoleLogger struct {
	BaseLogTarget
}

// Fire - log entry handler.
func (logger ConsoleLogger) Fire(entry *logrus.Entry) error {
	if !logger.Enable {
		return nil
	}

	msgBytes, err := logger.formatter.Format(entry)
	if err == nil {
		fmt.Fprintf(os.Stderr, string(msgBytes))
	}

	return err
}

// String - represents ConsoleLogger as string.
func (logger ConsoleLogger) String() string {
	enableStr := "disabled"
	if logger.Enable {
		enableStr = "enabled"
	}

	return fmt.Sprintf("console:%s", enableStr)
}

// NewConsoleLogger - return new console logger object.
func NewConsoleLogger() (logger ConsoleLogger) {
	logger.Enable = true
	logger.formatter = new(logrus.TextFormatter)

	return logger
}

// InitConsoleLogger - initializes console logger.
func InitConsoleLogger(logger *ConsoleLogger) {
	if !logger.Enable {
		return
	}

	if logger.formatter == nil {
		logger.formatter = new(logrus.TextFormatter)
	}

	return
}
