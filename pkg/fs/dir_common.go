package fs

import (
	"os"
	"time"
)

type Dirent struct {
	Name         string
	ModifiedTime time.Time // On unix this is empty.
	Size         int64     // On unix this is empty.
	IsDir        bool
}

type Dirents []Dirent

func (d Dirents) Len() int      { return len(d) }
func (d Dirents) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d Dirents) Less(i, j int) bool {
	n1 := d[i].Name
	if d[i].IsDir {
		n1 = n1 + string(os.PathSeparator)
	}

	n2 := d[j].Name
	if d[j].IsDir {
		n2 = n2 + string(os.PathSeparator)
	}

	return n1 < n2
}
