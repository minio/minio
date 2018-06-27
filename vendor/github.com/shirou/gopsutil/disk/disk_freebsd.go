// +build freebsd

package disk

import (
	"bytes"
	"context"
	"encoding/binary"
	"path"
	"strconv"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/shirou/gopsutil/internal/common"
)

func Partitions(all bool) ([]PartitionStat, error) {
	return PartitionsWithContext(context.Background(), all)
}

func PartitionsWithContext(ctx context.Context, all bool) ([]PartitionStat, error) {
	var ret []PartitionStat

	// get length
	count, err := unix.Getfsstat(nil, MNT_WAIT)
	if err != nil {
		return ret, err
	}

	fs := make([]Statfs, count)
	_, err = Getfsstat(fs, MNT_WAIT)

	for _, stat := range fs {
		opts := "rw"
		if stat.Flags&MNT_RDONLY != 0 {
			opts = "ro"
		}
		if stat.Flags&MNT_SYNCHRONOUS != 0 {
			opts += ",sync"
		}
		if stat.Flags&MNT_NOEXEC != 0 {
			opts += ",noexec"
		}
		if stat.Flags&MNT_NOSUID != 0 {
			opts += ",nosuid"
		}
		if stat.Flags&MNT_UNION != 0 {
			opts += ",union"
		}
		if stat.Flags&MNT_ASYNC != 0 {
			opts += ",async"
		}
		if stat.Flags&MNT_SUIDDIR != 0 {
			opts += ",suiddir"
		}
		if stat.Flags&MNT_SOFTDEP != 0 {
			opts += ",softdep"
		}
		if stat.Flags&MNT_NOSYMFOLLOW != 0 {
			opts += ",nosymfollow"
		}
		if stat.Flags&MNT_GJOURNAL != 0 {
			opts += ",gjounalc"
		}
		if stat.Flags&MNT_MULTILABEL != 0 {
			opts += ",multilabel"
		}
		if stat.Flags&MNT_ACLS != 0 {
			opts += ",acls"
		}
		if stat.Flags&MNT_NOATIME != 0 {
			opts += ",noattime"
		}
		if stat.Flags&MNT_NOCLUSTERR != 0 {
			opts += ",nocluster"
		}
		if stat.Flags&MNT_NOCLUSTERW != 0 {
			opts += ",noclusterw"
		}
		if stat.Flags&MNT_NFS4ACLS != 0 {
			opts += ",nfs4acls"
		}

		d := PartitionStat{
			Device:     common.IntToString(stat.Mntfromname[:]),
			Mountpoint: common.IntToString(stat.Mntonname[:]),
			Fstype:     common.IntToString(stat.Fstypename[:]),
			Opts:       opts,
		}
		if all == false {
			if !path.IsAbs(d.Device) || !common.PathExists(d.Device) {
				continue
			}
		}

		ret = append(ret, d)
	}

	return ret, nil
}

func IOCounters(names ...string) (map[string]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), names...)
}

func IOCountersWithContext(ctx context.Context, names ...string) (map[string]IOCountersStat, error) {
	// statinfo->devinfo->devstat
	// /usr/include/devinfo.h
	ret := make(map[string]IOCountersStat)

	r, err := unix.Sysctl("kern.devstat.all")
	if err != nil {
		return nil, err
	}
	buf := []byte(r)
	length := len(buf)

	count := int(uint64(length) / uint64(sizeOfDevstat))

	buf = buf[8:] // devstat.all has version in the head.
	// parse buf to Devstat
	for i := 0; i < count; i++ {
		b := buf[i*sizeOfDevstat : i*sizeOfDevstat+sizeOfDevstat]
		d, err := parseDevstat(b)
		if err != nil {
			continue
		}
		un := strconv.Itoa(int(d.Unit_number))
		name := common.IntToString(d.Device_name[:]) + un

		if len(names) > 0 && !common.StringsHas(names, name) {
			continue
		}

		ds := IOCountersStat{
			ReadCount:  d.Operations[DEVSTAT_READ],
			WriteCount: d.Operations[DEVSTAT_WRITE],
			ReadBytes:  d.Bytes[DEVSTAT_READ],
			WriteBytes: d.Bytes[DEVSTAT_WRITE],
			ReadTime:   uint64(d.Duration[DEVSTAT_READ].Compute() * 1000),
			WriteTime:  uint64(d.Duration[DEVSTAT_WRITE].Compute() * 1000),
			IoTime:     uint64(d.Busy_time.Compute() * 1000),
			Name:       name,
		}
		ret[name] = ds
	}

	return ret, nil
}

func (b Bintime) Compute() float64 {
	BINTIME_SCALE := 5.42101086242752217003726400434970855712890625e-20
	return float64(b.Sec) + float64(b.Frac)*BINTIME_SCALE
}

// BT2LD(time)     ((long double)(time).sec + (time).frac * BINTIME_SCALE)

// Getfsstat is borrowed from pkg/syscall/syscall_freebsd.go
// change Statfs_t to Statfs in order to get more information
func Getfsstat(buf []Statfs, flags int) (n int, err error) {
	return GetfsstatWithContext(context.Background(), buf, flags)
}

func GetfsstatWithContext(ctx context.Context, buf []Statfs, flags int) (n int, err error) {
	var _p0 unsafe.Pointer
	var bufsize uintptr
	if len(buf) > 0 {
		_p0 = unsafe.Pointer(&buf[0])
		bufsize = unsafe.Sizeof(Statfs{}) * uintptr(len(buf))
	}
	r0, _, e1 := unix.Syscall(unix.SYS_GETFSSTAT, uintptr(_p0), bufsize, uintptr(flags))
	n = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}

func parseDevstat(buf []byte) (Devstat, error) {
	var ds Devstat
	br := bytes.NewReader(buf)
	//	err := binary.Read(br, binary.LittleEndian, &ds)
	err := common.Read(br, binary.LittleEndian, &ds)
	if err != nil {
		return ds, err
	}

	return ds, nil
}

func getFsType(stat unix.Statfs_t) string {
	return common.IntToString(stat.Fstypename[:])
}
