// +build freebsd linux darwin

package disk

import (
	"context"

	"golang.org/x/sys/unix"
)

// Usage returns a file system usage. path is a filessytem path such
// as "/", not device file path like "/dev/vda1".  If you want to use
// a return value of disk.Partitions, use "Mountpoint" not "Device".
func Usage(path string) (*UsageStat, error) {
	return UsageWithContext(context.Background(), path)
}

func UsageWithContext(ctx context.Context, path string) (*UsageStat, error) {
	stat := unix.Statfs_t{}
	err := unix.Statfs(path, &stat)
	if err != nil {
		return nil, err
	}
	bsize := stat.Bsize

	ret := &UsageStat{
		Path:        path,
		Fstype:      getFsType(stat),
		Total:       (uint64(stat.Blocks) * uint64(bsize)),
		Free:        (uint64(stat.Bavail) * uint64(bsize)),
		InodesTotal: (uint64(stat.Files)),
		InodesFree:  (uint64(stat.Ffree)),
	}

	// if could not get InodesTotal, return empty
	if ret.InodesTotal < ret.InodesFree {
		return ret, nil
	}

	ret.InodesUsed = (ret.InodesTotal - ret.InodesFree)
	ret.Used = (uint64(stat.Blocks) - uint64(stat.Bfree)) * uint64(bsize)

	if ret.InodesTotal == 0 {
		ret.InodesUsedPercent = 0
	} else {
		ret.InodesUsedPercent = (float64(ret.InodesUsed) / float64(ret.InodesTotal)) * 100.0
	}

	if ret.Total == 0 {
		ret.UsedPercent = 0
	} else {
		ret.UsedPercent = (float64(ret.Used) / float64(ret.Total)) * 100.0
	}

	return ret, nil
}
