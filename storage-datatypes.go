package main

// VolInfo - volume info
import "time"

type VolInfo struct {
	Name    string
	Created time.Time
}

// FileInfo - file stat information.
type FileInfo struct {
	Volume       string
	Name         string
	ModifiedTime time.Time
	Size         int64
	IsDir        bool
}
