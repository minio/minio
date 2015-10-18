package fs

import (
	"os"
	"time"
)

// AutoExpiryThread - auto expiry thread
func (fs Filesystem) AutoExpiryThread(expiry time.Duration) {
	expireFiles := func(fp string, fl os.FileInfo, err error) error {
		if fp == fs.path {
			return nil
		}
		if fl.Mode().IsRegular() || fl.Mode()&os.ModeSymlink == os.ModeSymlink {
			if time.Now().Sub(fl.ModTime()) > expiry {
				if err := os.Remove(fp); err != nil {
					if os.IsNotExist(err) {
						return nil
					}
					return err
				}
			}
			return ErrDirNotEmpty
		}
		return nil
	}
	ticker := time.NewTicker(3 * time.Hour)
	for {
		select {
		// TODO - add a way to stop the timer thread
		case <-ticker.C:
			err := WalkUnsorted(fs.path, expireFiles)
			if err != nil {
				if !os.IsNotExist(err) && err != ErrDirNotEmpty {
					ticker.Stop()
					return
				}
			}
		}
	}
}
