// +build solaris

package disk

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/shirou/gopsutil/internal/common"
	"golang.org/x/sys/unix"
)

const (
	// _DEFAULT_NUM_MOUNTS is set to `cat /etc/mnttab | wc -l` rounded up to the
	// nearest power of two.
	_DEFAULT_NUM_MOUNTS = 32

	// _MNTTAB default place to read mount information
	_MNTTAB = "/etc/mnttab"
)

var (
	// A blacklist of read-only virtual filesystems.  Writable filesystems are of
	// operational concern and must not be included in this list.
	fsTypeBlacklist = map[string]struct{}{
		"ctfs":   struct{}{},
		"dev":    struct{}{},
		"fd":     struct{}{},
		"lofs":   struct{}{},
		"lxproc": struct{}{},
		"mntfs":  struct{}{},
		"objfs":  struct{}{},
		"proc":   struct{}{},
	}
)

func Partitions(all bool) ([]PartitionStat, error) {
	return PartitionsWithContext(context.Background(), all)
}

func PartitionsWithContext(ctx context.Context, all bool) ([]PartitionStat, error) {
	ret := make([]PartitionStat, 0, _DEFAULT_NUM_MOUNTS)

	// Scan mnttab(4)
	f, err := os.Open(_MNTTAB)
	if err != nil {
	}
	defer func() {
		if err == nil {
			err = f.Close()
		} else {
			f.Close()
		}
	}()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")

		if _, found := fsTypeBlacklist[fields[2]]; found {
			continue
		}

		ret = append(ret, PartitionStat{
			// NOTE(seanc@): Device isn't exactly accurate: from mnttab(4): "The name
			// of the resource that has been mounted."  Ideally this value would come
			// from Statvfs_t.Fsid but I'm leaving it to the caller to traverse
			// unix.Statvfs().
			Device:     fields[0],
			Mountpoint: fields[1],
			Fstype:     fields[2],
			Opts:       fields[3],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan %q: %v", _MNTTAB, err)
	}

	return ret, err
}

func IOCounters(names ...string) (map[string]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), names...)
}

func IOCountersWithContext(ctx context.Context, names ...string) (map[string]IOCountersStat, error) {
	return nil, common.ErrNotImplementedError
}

func Usage(path string) (*UsageStat, error) {
	return UsageWithContext(context.Background(), path)
}

func UsageWithContext(ctx context.Context, path string) (*UsageStat, error) {
	statvfs := unix.Statvfs_t{}
	if err := unix.Statvfs(path, &statvfs); err != nil {
		return nil, fmt.Errorf("unable to call statvfs(2) on %q: %v", path, err)
	}

	usageStat := &UsageStat{
		Path:   path,
		Fstype: common.IntToString(statvfs.Basetype[:]),
		Total:  statvfs.Blocks * statvfs.Frsize,
		Free:   statvfs.Bfree * statvfs.Frsize,
		Used:   (statvfs.Blocks - statvfs.Bfree) * statvfs.Frsize,

		// NOTE: ZFS (and FreeBZSD's UFS2) use dynamic inode/dnode allocation.
		// Explicitly return a near-zero value for InodesUsedPercent so that nothing
		// attempts to garbage collect based on a lack of available inodes/dnodes.
		// Similarly, don't use the zero value to prevent divide-by-zero situations
		// and inject a faux near-zero value.  Filesystems evolve.  Has your
		// filesystem evolved?  Probably not if you care about the number of
		// available inodes.
		InodesTotal:       1024.0 * 1024.0,
		InodesUsed:        1024.0,
		InodesFree:        math.MaxUint64,
		InodesUsedPercent: (1024.0 / (1024.0 * 1024.0)) * 100.0,
	}

	usageStat.UsedPercent = (float64(usageStat.Used) / float64(usageStat.Total)) * 100.0

	return usageStat, nil
}
