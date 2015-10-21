// +build !windows

/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"log/syslog"

	"github.com/Sirupsen/logrus"
	"github.com/minio/minio-xl/pkg/probe"
)

// syslogHook to send logs via syslog.
type syslogHook struct {
	writer        *syslog.Writer
	syslogNetwork string
	syslogRaddr   string
}

func log2Syslog(network, raddr string) *probe.Error {
	syslogHook, e := newSyslog(network, raddr, syslog.LOG_ERR, "MINIO")
	if e != nil {
		return probe.NewError(e)
	}
	log.Hooks.Add(syslogHook)               // Add syslog hook.
	log.Formatter = &logrus.JSONFormatter{} // JSON formatted log.
	log.Level = logrus.InfoLevel            // Minimum log level.
	return nil
}

// newSyslog - Creates a hook to be added to an instance of logger.
func newSyslog(network, raddr string, priority syslog.Priority, tag string) (*syslogHook, error) {
	w, err := syslog.Dial(network, raddr, priority, tag)
	return &syslogHook{w, network, raddr}, err
}

// Fire - fire the log event
func (hook *syslogHook) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		return fmt.Errorf("Unable to read entry, %v", err)
	}
	switch entry.Level {
	case logrus.PanicLevel:
		return hook.writer.Crit(line)
	case logrus.FatalLevel:
		return hook.writer.Crit(line)
	case logrus.ErrorLevel:
		return hook.writer.Err(line)
	case logrus.WarnLevel:
		return hook.writer.Warning(line)
	case logrus.InfoLevel:
		return hook.writer.Info(line)
	case logrus.DebugLevel:
		return hook.writer.Debug(line)
	default:
		return nil
	}
}

// Levels -
func (hook *syslogHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
