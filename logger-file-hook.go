package main

import (
	"fmt"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/minio/minio-xl/pkg/probe"
)

type localFile struct {
	*os.File
}

func log2File(filename string) *probe.Error {
	fileHook, e := newFile(filename)
	if e != nil {
		return probe.NewError(e)
	}
	log.Hooks.Add(fileHook)                 // Add a local file hook.
	log.Formatter = &logrus.JSONFormatter{} // JSON formatted log.
	log.Level = logrus.InfoLevel            // Minimum log level.
	return nil
}

func newFile(filename string) (*localFile, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &localFile{file}, nil
}

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
