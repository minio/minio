package main

import (
	"io"
	"os"
	"sort"
	"strings"
)

const PathSeparatorString = string(os.PathSeparator)

func IsDirEmpty(dirname string) (status bool, err error) {
	f, err := os.Open(dirname)
	if err == nil {
		defer f.Close()
		if _, err = f.Readdirnames(1); err == io.EOF {
			status = true
			err = nil
		}
	}

	return
}

func IsDirExist(dirname string) (status bool, err error) {
	fi, err := os.Lstat(dirname)
	if err == nil {
		status = fi.IsDir()
	}

	return
}

// byName implements sort.Interface for sorting os.FileInfo list
type byName []os.FileInfo

func (f byName) Len() int      { return len(f) }
func (f byName) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f byName) Less(i, j int) bool {
	n1 := f[i].Name()
	if f[i].IsDir() {
		n1 = n1 + PathSeparatorString
	}

	n2 := f[j].Name()
	if f[j].IsDir() {
		n2 = n2 + PathSeparatorString
	}

	return n1 < n2
}

func readDir(dirname string) (fi []os.FileInfo, err error) {
	f, err := os.Open(dirname)
	if err == nil {
		defer f.Close()
		if fi, err = f.Readdir(-1); fi != nil {
			sort.Sort(byName(fi))
		}
	}

	return
}

func filteredReadDir(scanDir, bucketDir, filterPrefix string) (ois []ObjectInfo, err error) {
	fis, err := readDir(scanDir)
	if err != nil {
		return
	}

	namePrefix := strings.Replace(strings.Replace(scanDir, PathSeparatorString, "/", -1),
		strings.Replace(bucketDir, PathSeparatorString, "/", -1), "", 1)
	if strings.HasPrefix(namePrefix, "/") {
		/* remove beginning "/" */
		namePrefix = namePrefix[1:]
	}

	for _, fi := range fis {
		name := fi.Name()
		if namePrefix != "" {
			name = namePrefix + "/" + name
		}

		if fi.IsDir() {
			name += "/"
		}

		// note: filterPrefix="" matches all too
		if strings.HasPrefix(name, filterPrefix) {
			ois = append(ois, ObjectInfo{
				Name:         name,
				ModifiedTime: fi.ModTime(),
				Checksum:     "",
				Size:         fi.Size(),
				IsDir:        fi.IsDir(),
			})
		}
	}

	return
}
