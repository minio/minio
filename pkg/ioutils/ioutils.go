package ioutils

import (
	"os"
	"sort"
)

// byName implements sort.Interface for sorting os.FileInfo list.
type byName []os.FileInfo

func (f byName) Len() int           { return len(f) }
func (f byName) Less(i, j int) bool { return f[i].Name() < f[j].Name() }
func (f byName) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

// ReadDirN reads the directory named by dirname and returns
// a list of sorted directory entries of size 'n'.
func ReadDirN(dirname string, n int) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdir(n)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Sort(byName(list))
	return list, nil
}

// ReadDirNamesN reads the directory named by dirname and returns
// a list of sorted directory names of size 'n'.
func ReadDirNamesN(dirname string, n int) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(n)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}
