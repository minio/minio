// +build windows

package watcher

import "os"

func SameFile(fi1, fi2 os.FileInfo) bool {
	return fi1.ModTime() == fi2.ModTime() &&
		fi1.Size() == fi2.Size() &&
		fi1.Mode() == fi2.Mode() &&
		fi1.IsDir() == fi2.IsDir()
}
