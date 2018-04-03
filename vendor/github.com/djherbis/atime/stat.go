// Package atime provides a platform-independent way to get atimes for files.
package atime

import (
	"os"
	"time"
)

// Get returns the Last Access Time for the given FileInfo
func Get(fi os.FileInfo) time.Time {
	return atime(fi)
}

// Stat returns the Last Access Time for the given filename
func Stat(name string) (time.Time, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return time.Time{}, err
	}
	return atime(fi), nil
}
