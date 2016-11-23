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

import "github.com/Sirupsen/logrus"

// consoleLogger - default logger if not other logging is enabled.
type consoleLogger struct {
	Enable bool   `json:"enable"`
	Level  string `json:"level"`
}

// enable console logger.
func enableConsoleLogger() {
	clogger := serverConfig.GetConsoleLogger()
	if !clogger.Enable {
		return
	}

	consoleLogger := logrus.New()

	// log.Out and log.Formatter use the default versions.
	// Only set specific log level.
	lvl, err := logrus.ParseLevel(clogger.Level)
	fatalIf(err, "Unknown log level found in the config file.")

	consoleLogger.Level = lvl
	consoleLogger.Formatter = new(logrus.TextFormatter)
	log.mu.Lock()
	log.loggers = append(log.loggers, consoleLogger)
	log.mu.Unlock()
}
