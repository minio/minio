package persist

import (
	"encoding/base32"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/NebulousLabs/fastrand"
)

const (
	// persistDir defines the folder that is used for testing the persist
	// package.
	persistDir = "persist"

	// tempSuffix is the suffix that is applied to the temporary/backup versions
	// of the files being persisted.
	tempSuffix = "_temp"
)

var (
	// ErrBadFilenameSuffix indicates that SaveJSON or LoadJSON was called using
	// a filename that has a bad suffix. This prevents users from trying to use
	// this package to manage the temp files - this packaage will manage them
	// automatically.
	ErrBadFilenameSuffix = errors.New("filename suffix not allowed")

	// ErrBadHeader indicates that the file opened is not the file that was
	// expected.
	ErrBadHeader = errors.New("wrong header")

	// ErrBadVersion indicates that the version number of the file is not
	// compatible with the current codebase.
	ErrBadVersion = errors.New("incompatible version")

	// ErrFileInUse is returned if SaveJSON or LoadJSON is called on a file
	// that's already being manipulated in another thread by the persist
	// package.
	ErrFileInUse = errors.New("another thread is saving or loading this file")
)

var (
	// activeFiles is a map tracking which filenames are currently being used
	// for saving and loading. There should never be a situation where the same
	// file is being called twice from different threads, as the persist package
	// has no way to tell what order they were intended to be called.
	activeFiles   = make(map[string]struct{})
	activeFilesMu sync.Mutex
)

// Metadata contains the header and version of the data being stored.
type Metadata struct {
	Header, Version string
}

// RandomSuffix returns a 20 character base32 suffix for a filename. There are
// 100 bits of entropy, and a very low probability of colliding with existing
// files unintentionally.
func RandomSuffix() string {
	str := base32.StdEncoding.EncodeToString(fastrand.Bytes(20))
	return str[:20]
}

// A safeFile is a file that is stored under a temporary filename. When Commit
// is called, the file is renamed to its "final" filename. This allows for
// atomic updating of files; otherwise, an unexpected shutdown could leave a
// valuable file in a corrupted state. Callers must still Close the file handle
// as usual.
type safeFile struct {
	*os.File
	finalName string
}

// CommitSync syncs the file, closes it, and then renames it to the intended
// final filename. CommitSync should not be called from a defer if the
// function it is being called from can return an error.
func (sf *safeFile) CommitSync() error {
	if err := sf.Sync(); err != nil {
		return err
	}
	if err := sf.Close(); err != nil {
		return err
	}
	return os.Rename(sf.finalName+"_temp", sf.finalName)
}

// NewSafeFile returns a file that can atomically be written to disk,
// minimizing the risk of corruption.
func NewSafeFile(filename string) (*safeFile, error) {
	file, err := os.Create(filename + "_temp")
	if err != nil {
		return nil, err
	}

	// Get the absolute path of the filename so that calling os.Chdir in
	// between calling NewSafeFile and calling safeFile.Commit does not change
	// the final file path.
	absFilename, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}

	return &safeFile{file, absFilename}, nil
}
