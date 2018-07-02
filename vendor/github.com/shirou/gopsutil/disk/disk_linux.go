// +build linux

package disk

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/shirou/gopsutil/internal/common"
)

const (
	SectorSize = 512
)
const (
	// man statfs
	ADFS_SUPER_MAGIC      = 0xadf5
	AFFS_SUPER_MAGIC      = 0xADFF
	BDEVFS_MAGIC          = 0x62646576
	BEFS_SUPER_MAGIC      = 0x42465331
	BFS_MAGIC             = 0x1BADFACE
	BINFMTFS_MAGIC        = 0x42494e4d
	BTRFS_SUPER_MAGIC     = 0x9123683E
	CGROUP_SUPER_MAGIC    = 0x27e0eb
	CIFS_MAGIC_NUMBER     = 0xFF534D42
	CODA_SUPER_MAGIC      = 0x73757245
	COH_SUPER_MAGIC       = 0x012FF7B7
	CRAMFS_MAGIC          = 0x28cd3d45
	DEBUGFS_MAGIC         = 0x64626720
	DEVFS_SUPER_MAGIC     = 0x1373
	DEVPTS_SUPER_MAGIC    = 0x1cd1
	EFIVARFS_MAGIC        = 0xde5e81e4
	EFS_SUPER_MAGIC       = 0x00414A53
	EXT_SUPER_MAGIC       = 0x137D
	EXT2_OLD_SUPER_MAGIC  = 0xEF51
	EXT2_SUPER_MAGIC      = 0xEF53
	EXT3_SUPER_MAGIC      = 0xEF53
	EXT4_SUPER_MAGIC      = 0xEF53
	FUSE_SUPER_MAGIC      = 0x65735546
	FUTEXFS_SUPER_MAGIC   = 0xBAD1DEA
	HFS_SUPER_MAGIC       = 0x4244
	HOSTFS_SUPER_MAGIC    = 0x00c0ffee
	HPFS_SUPER_MAGIC      = 0xF995E849
	HUGETLBFS_MAGIC       = 0x958458f6
	ISOFS_SUPER_MAGIC     = 0x9660
	JFFS2_SUPER_MAGIC     = 0x72b6
	JFS_SUPER_MAGIC       = 0x3153464a
	MINIX_SUPER_MAGIC     = 0x137F /* orig. minix */
	MINIX_SUPER_MAGIC2    = 0x138F /* 30 char minix */
	MINIX2_SUPER_MAGIC    = 0x2468 /* minix V2 */
	MINIX2_SUPER_MAGIC2   = 0x2478 /* minix V2, 30 char names */
	MINIX3_SUPER_MAGIC    = 0x4d5a /* minix V3 fs, 60 char names */
	MQUEUE_MAGIC          = 0x19800202
	MSDOS_SUPER_MAGIC     = 0x4d44
	NCP_SUPER_MAGIC       = 0x564c
	NFS_SUPER_MAGIC       = 0x6969
	NILFS_SUPER_MAGIC     = 0x3434
	NTFS_SB_MAGIC         = 0x5346544e
	OCFS2_SUPER_MAGIC     = 0x7461636f
	OPENPROM_SUPER_MAGIC  = 0x9fa1
	PIPEFS_MAGIC          = 0x50495045
	PROC_SUPER_MAGIC      = 0x9fa0
	PSTOREFS_MAGIC        = 0x6165676C
	QNX4_SUPER_MAGIC      = 0x002f
	QNX6_SUPER_MAGIC      = 0x68191122
	RAMFS_MAGIC           = 0x858458f6
	REISERFS_SUPER_MAGIC  = 0x52654973
	ROMFS_MAGIC           = 0x7275
	SELINUX_MAGIC         = 0xf97cff8c
	SMACK_MAGIC           = 0x43415d53
	SMB_SUPER_MAGIC       = 0x517B
	SOCKFS_MAGIC          = 0x534F434B
	SQUASHFS_MAGIC        = 0x73717368
	SYSFS_MAGIC           = 0x62656572
	SYSV2_SUPER_MAGIC     = 0x012FF7B6
	SYSV4_SUPER_MAGIC     = 0x012FF7B5
	TMPFS_MAGIC           = 0x01021994
	UDF_SUPER_MAGIC       = 0x15013346
	UFS_MAGIC             = 0x00011954
	USBDEVICE_SUPER_MAGIC = 0x9fa2
	V9FS_MAGIC            = 0x01021997
	VXFS_SUPER_MAGIC      = 0xa501FCF5
	XENFS_SUPER_MAGIC     = 0xabba1974
	XENIX_SUPER_MAGIC     = 0x012FF7B4
	XFS_SUPER_MAGIC       = 0x58465342
	_XIAFS_SUPER_MAGIC    = 0x012FD16D

	AFS_SUPER_MAGIC             = 0x5346414F
	AUFS_SUPER_MAGIC            = 0x61756673
	ANON_INODE_FS_SUPER_MAGIC   = 0x09041934
	CEPH_SUPER_MAGIC            = 0x00C36400
	ECRYPTFS_SUPER_MAGIC        = 0xF15F
	FAT_SUPER_MAGIC             = 0x4006
	FHGFS_SUPER_MAGIC           = 0x19830326
	FUSEBLK_SUPER_MAGIC         = 0x65735546
	FUSECTL_SUPER_MAGIC         = 0x65735543
	GFS_SUPER_MAGIC             = 0x1161970
	GPFS_SUPER_MAGIC            = 0x47504653
	MTD_INODE_FS_SUPER_MAGIC    = 0x11307854
	INOTIFYFS_SUPER_MAGIC       = 0x2BAD1DEA
	ISOFS_R_WIN_SUPER_MAGIC     = 0x4004
	ISOFS_WIN_SUPER_MAGIC       = 0x4000
	JFFS_SUPER_MAGIC            = 0x07C0
	KAFS_SUPER_MAGIC            = 0x6B414653
	LUSTRE_SUPER_MAGIC          = 0x0BD00BD0
	NFSD_SUPER_MAGIC            = 0x6E667364
	PANFS_SUPER_MAGIC           = 0xAAD7AAEA
	RPC_PIPEFS_SUPER_MAGIC      = 0x67596969
	SECURITYFS_SUPER_MAGIC      = 0x73636673
	UFS_BYTESWAPPED_SUPER_MAGIC = 0x54190100
	VMHGFS_SUPER_MAGIC          = 0xBACBACBC
	VZFS_SUPER_MAGIC            = 0x565A4653
	ZFS_SUPER_MAGIC             = 0x2FC12FC1
)

// coreutils/src/stat.c
var fsTypeMap = map[int64]string{
	ADFS_SUPER_MAGIC:          "adfs",          /* 0xADF5 local */
	AFFS_SUPER_MAGIC:          "affs",          /* 0xADFF local */
	AFS_SUPER_MAGIC:           "afs",           /* 0x5346414F remote */
	ANON_INODE_FS_SUPER_MAGIC: "anon-inode FS", /* 0x09041934 local */
	AUFS_SUPER_MAGIC:          "aufs",          /* 0x61756673 remote */
	//	AUTOFS_SUPER_MAGIC:          "autofs",              /* 0x0187 local */
	BEFS_SUPER_MAGIC:            "befs",                /* 0x42465331 local */
	BDEVFS_MAGIC:                "bdevfs",              /* 0x62646576 local */
	BFS_MAGIC:                   "bfs",                 /* 0x1BADFACE local */
	BINFMTFS_MAGIC:              "binfmt_misc",         /* 0x42494E4D local */
	BTRFS_SUPER_MAGIC:           "btrfs",               /* 0x9123683E local */
	CEPH_SUPER_MAGIC:            "ceph",                /* 0x00C36400 remote */
	CGROUP_SUPER_MAGIC:          "cgroupfs",            /* 0x0027E0EB local */
	CIFS_MAGIC_NUMBER:           "cifs",                /* 0xFF534D42 remote */
	CODA_SUPER_MAGIC:            "coda",                /* 0x73757245 remote */
	COH_SUPER_MAGIC:             "coh",                 /* 0x012FF7B7 local */
	CRAMFS_MAGIC:                "cramfs",              /* 0x28CD3D45 local */
	DEBUGFS_MAGIC:               "debugfs",             /* 0x64626720 local */
	DEVFS_SUPER_MAGIC:           "devfs",               /* 0x1373 local */
	DEVPTS_SUPER_MAGIC:          "devpts",              /* 0x1CD1 local */
	ECRYPTFS_SUPER_MAGIC:        "ecryptfs",            /* 0xF15F local */
	EFS_SUPER_MAGIC:             "efs",                 /* 0x00414A53 local */
	EXT_SUPER_MAGIC:             "ext",                 /* 0x137D local */
	EXT2_SUPER_MAGIC:            "ext2/ext3",           /* 0xEF53 local */
	EXT2_OLD_SUPER_MAGIC:        "ext2",                /* 0xEF51 local */
	FAT_SUPER_MAGIC:             "fat",                 /* 0x4006 local */
	FHGFS_SUPER_MAGIC:           "fhgfs",               /* 0x19830326 remote */
	FUSEBLK_SUPER_MAGIC:         "fuseblk",             /* 0x65735546 remote */
	FUSECTL_SUPER_MAGIC:         "fusectl",             /* 0x65735543 remote */
	FUTEXFS_SUPER_MAGIC:         "futexfs",             /* 0x0BAD1DEA local */
	GFS_SUPER_MAGIC:             "gfs/gfs2",            /* 0x1161970 remote */
	GPFS_SUPER_MAGIC:            "gpfs",                /* 0x47504653 remote */
	HFS_SUPER_MAGIC:             "hfs",                 /* 0x4244 local */
	HPFS_SUPER_MAGIC:            "hpfs",                /* 0xF995E849 local */
	HUGETLBFS_MAGIC:             "hugetlbfs",           /* 0x958458F6 local */
	MTD_INODE_FS_SUPER_MAGIC:    "inodefs",             /* 0x11307854 local */
	INOTIFYFS_SUPER_MAGIC:       "inotifyfs",           /* 0x2BAD1DEA local */
	ISOFS_SUPER_MAGIC:           "isofs",               /* 0x9660 local */
	ISOFS_R_WIN_SUPER_MAGIC:     "isofs",               /* 0x4004 local */
	ISOFS_WIN_SUPER_MAGIC:       "isofs",               /* 0x4000 local */
	JFFS_SUPER_MAGIC:            "jffs",                /* 0x07C0 local */
	JFFS2_SUPER_MAGIC:           "jffs2",               /* 0x72B6 local */
	JFS_SUPER_MAGIC:             "jfs",                 /* 0x3153464A local */
	KAFS_SUPER_MAGIC:            "k-afs",               /* 0x6B414653 remote */
	LUSTRE_SUPER_MAGIC:          "lustre",              /* 0x0BD00BD0 remote */
	MINIX_SUPER_MAGIC:           "minix",               /* 0x137F local */
	MINIX_SUPER_MAGIC2:          "minix (30 char.)",    /* 0x138F local */
	MINIX2_SUPER_MAGIC:          "minix v2",            /* 0x2468 local */
	MINIX2_SUPER_MAGIC2:         "minix v2 (30 char.)", /* 0x2478 local */
	MINIX3_SUPER_MAGIC:          "minix3",              /* 0x4D5A local */
	MQUEUE_MAGIC:                "mqueue",              /* 0x19800202 local */
	MSDOS_SUPER_MAGIC:           "msdos",               /* 0x4D44 local */
	NCP_SUPER_MAGIC:             "novell",              /* 0x564C remote */
	NFS_SUPER_MAGIC:             "nfs",                 /* 0x6969 remote */
	NFSD_SUPER_MAGIC:            "nfsd",                /* 0x6E667364 remote */
	NILFS_SUPER_MAGIC:           "nilfs",               /* 0x3434 local */
	NTFS_SB_MAGIC:               "ntfs",                /* 0x5346544E local */
	OPENPROM_SUPER_MAGIC:        "openprom",            /* 0x9FA1 local */
	OCFS2_SUPER_MAGIC:           "ocfs2",               /* 0x7461636f remote */
	PANFS_SUPER_MAGIC:           "panfs",               /* 0xAAD7AAEA remote */
	PIPEFS_MAGIC:                "pipefs",              /* 0x50495045 remote */
	PROC_SUPER_MAGIC:            "proc",                /* 0x9FA0 local */
	PSTOREFS_MAGIC:              "pstorefs",            /* 0x6165676C local */
	QNX4_SUPER_MAGIC:            "qnx4",                /* 0x002F local */
	QNX6_SUPER_MAGIC:            "qnx6",                /* 0x68191122 local */
	RAMFS_MAGIC:                 "ramfs",               /* 0x858458F6 local */
	REISERFS_SUPER_MAGIC:        "reiserfs",            /* 0x52654973 local */
	ROMFS_MAGIC:                 "romfs",               /* 0x7275 local */
	RPC_PIPEFS_SUPER_MAGIC:      "rpc_pipefs",          /* 0x67596969 local */
	SECURITYFS_SUPER_MAGIC:      "securityfs",          /* 0x73636673 local */
	SELINUX_MAGIC:               "selinux",             /* 0xF97CFF8C local */
	SMB_SUPER_MAGIC:             "smb",                 /* 0x517B remote */
	SOCKFS_MAGIC:                "sockfs",              /* 0x534F434B local */
	SQUASHFS_MAGIC:              "squashfs",            /* 0x73717368 local */
	SYSFS_MAGIC:                 "sysfs",               /* 0x62656572 local */
	SYSV2_SUPER_MAGIC:           "sysv2",               /* 0x012FF7B6 local */
	SYSV4_SUPER_MAGIC:           "sysv4",               /* 0x012FF7B5 local */
	TMPFS_MAGIC:                 "tmpfs",               /* 0x01021994 local */
	UDF_SUPER_MAGIC:             "udf",                 /* 0x15013346 local */
	UFS_MAGIC:                   "ufs",                 /* 0x00011954 local */
	UFS_BYTESWAPPED_SUPER_MAGIC: "ufs",                 /* 0x54190100 local */
	USBDEVICE_SUPER_MAGIC:       "usbdevfs",            /* 0x9FA2 local */
	V9FS_MAGIC:                  "v9fs",                /* 0x01021997 local */
	VMHGFS_SUPER_MAGIC:          "vmhgfs",              /* 0xBACBACBC remote */
	VXFS_SUPER_MAGIC:            "vxfs",                /* 0xA501FCF5 local */
	VZFS_SUPER_MAGIC:            "vzfs",                /* 0x565A4653 local */
	XENFS_SUPER_MAGIC:           "xenfs",               /* 0xABBA1974 local */
	XENIX_SUPER_MAGIC:           "xenix",               /* 0x012FF7B4 local */
	XFS_SUPER_MAGIC:             "xfs",                 /* 0x58465342 local */
	_XIAFS_SUPER_MAGIC:          "xia",                 /* 0x012FD16D local */
	ZFS_SUPER_MAGIC:             "zfs",                 /* 0x2FC12FC1 local */
}

// Partitions returns disk partitions. If all is false, returns
// physical devices only (e.g. hard disks, cd-rom drives, USB keys)
// and ignore all others (e.g. memory partitions such as /dev/shm)
func Partitions(all bool) ([]PartitionStat, error) {
	return PartitionsWithContext(context.Background(), all)
}

func PartitionsWithContext(ctx context.Context, all bool) ([]PartitionStat, error) {
	filename := common.HostProc("self/mounts")
	lines, err := common.ReadLines(filename)
	if err != nil {
		return nil, err
	}

	fs, err := getFileSystems()
	if err != nil {
		return nil, err
	}

	ret := make([]PartitionStat, 0, len(lines))

	for _, line := range lines {
		fields := strings.Fields(line)
		d := PartitionStat{
			Device:     fields[0],
			Mountpoint: fields[1],
			Fstype:     fields[2],
			Opts:       fields[3],
		}
		if all == false {
			if d.Device == "none" || !common.StringsHas(fs, d.Fstype) {
				continue
			}
		}
		ret = append(ret, d)
	}

	return ret, nil
}

// getFileSystems returns supported filesystems from /proc/filesystems
func getFileSystems() ([]string, error) {
	filename := common.HostProc("filesystems")
	lines, err := common.ReadLines(filename)
	if err != nil {
		return nil, err
	}
	var ret []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "nodev") {
			ret = append(ret, strings.TrimSpace(line))
			continue
		}
		t := strings.Split(line, "\t")
		if len(t) != 2 || t[1] != "zfs" {
			continue
		}
		ret = append(ret, strings.TrimSpace(t[1]))
	}

	return ret, nil
}

func IOCounters(names ...string) (map[string]IOCountersStat, error) {
	return IOCountersWithContext(context.Background(), names...)
}

func IOCountersWithContext(ctx context.Context, names ...string) (map[string]IOCountersStat, error) {
	filename := common.HostProc("diskstats")
	lines, err := common.ReadLines(filename)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]IOCountersStat, 0)
	empty := IOCountersStat{}

	// use only basename such as "/dev/sda1" to "sda1"
	for i, name := range names {
		names[i] = filepath.Base(name)
	}

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 14 {
			// malformed line in /proc/diskstats, avoid panic by ignoring.
			continue
		}
		name := fields[2]

		if len(names) > 0 && !common.StringsHas(names, name) {
			continue
		}

		reads, err := strconv.ParseUint((fields[3]), 10, 64)
		if err != nil {
			return ret, err
		}
		mergedReads, err := strconv.ParseUint((fields[4]), 10, 64)
		if err != nil {
			return ret, err
		}
		rbytes, err := strconv.ParseUint((fields[5]), 10, 64)
		if err != nil {
			return ret, err
		}
		rtime, err := strconv.ParseUint((fields[6]), 10, 64)
		if err != nil {
			return ret, err
		}
		writes, err := strconv.ParseUint((fields[7]), 10, 64)
		if err != nil {
			return ret, err
		}
		mergedWrites, err := strconv.ParseUint((fields[8]), 10, 64)
		if err != nil {
			return ret, err
		}
		wbytes, err := strconv.ParseUint((fields[9]), 10, 64)
		if err != nil {
			return ret, err
		}
		wtime, err := strconv.ParseUint((fields[10]), 10, 64)
		if err != nil {
			return ret, err
		}
		iopsInProgress, err := strconv.ParseUint((fields[11]), 10, 64)
		if err != nil {
			return ret, err
		}
		iotime, err := strconv.ParseUint((fields[12]), 10, 64)
		if err != nil {
			return ret, err
		}
		weightedIO, err := strconv.ParseUint((fields[13]), 10, 64)
		if err != nil {
			return ret, err
		}
		d := IOCountersStat{
			ReadBytes:        rbytes * SectorSize,
			WriteBytes:       wbytes * SectorSize,
			ReadCount:        reads,
			WriteCount:       writes,
			MergedReadCount:  mergedReads,
			MergedWriteCount: mergedWrites,
			ReadTime:         rtime,
			WriteTime:        wtime,
			IopsInProgress:   iopsInProgress,
			IoTime:           iotime,
			WeightedIO:       weightedIO,
		}
		if d == empty {
			continue
		}
		d.Name = name

		d.SerialNumber = GetDiskSerialNumber(name)
		ret[name] = d
	}
	return ret, nil
}

// GetDiskSerialNumber returns Serial Number of given device or empty string
// on error. Name of device is expected, eg. /dev/sda
func GetDiskSerialNumber(name string) string {
	return GetDiskSerialNumberWithContext(context.Background(), name)
}

func GetDiskSerialNumberWithContext(ctx context.Context, name string) string {
	n := fmt.Sprintf("--name=%s", name)
	udevadm, err := exec.LookPath("/sbin/udevadm")
	if err != nil {
		return ""
	}

	out, err := invoke.CommandWithContext(ctx, udevadm, "info", "--query=property", n)

	// does not return error, just an empty string
	if err != nil {
		return ""
	}
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		values := strings.Split(line, "=")
		if len(values) < 2 || values[0] != "ID_SERIAL" {
			// only get ID_SERIAL, not ID_SERIAL_SHORT
			continue
		}
		return values[1]
	}
	return ""
}

func getFsType(stat unix.Statfs_t) string {
	t := int64(stat.Type)
	ret, ok := fsTypeMap[t]
	if !ok {
		return ""
	}
	return ret
}
