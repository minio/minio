package main

import (
	"os"
	"time"
)

// VolInfo - volume info
type VolInfo struct {
	Name    string
	Created time.Time
}

// FileInfo - file stat information.
type FileInfo struct {
	Volume  string
	Name    string
	ModTime time.Time
	Size    int64
	Mode    os.FileMode
}
