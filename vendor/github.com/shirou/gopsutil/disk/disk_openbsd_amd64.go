// Created by cgo -godefs - DO NOT EDIT
// cgo -godefs types_openbsd.go

package disk

const (
	sizeofPtr        = 0x8
	sizeofShort      = 0x2
	sizeofInt        = 0x4
	sizeofLong       = 0x8
	sizeofLongLong   = 0x8
	sizeofLongDouble = 0x8

	DEVSTAT_NO_DATA = 0x00
	DEVSTAT_READ    = 0x01
	DEVSTAT_WRITE   = 0x02
	DEVSTAT_FREE    = 0x03

	MNT_RDONLY      = 0x00000001
	MNT_SYNCHRONOUS = 0x00000002
	MNT_NOEXEC      = 0x00000004
	MNT_NOSUID      = 0x00000008
	MNT_NODEV       = 0x00000010
	MNT_ASYNC       = 0x00000040

	MNT_WAIT   = 1
	MNT_NOWAIT = 2
	MNT_LAZY   = 3
)

const (
	sizeOfDiskstats = 0x70
)

type (
	_C_short       int16
	_C_int         int32
	_C_long        int64
	_C_long_long   int64
	_C_long_double int64
)

type Statfs struct {
	F_flags       uint32
	F_bsize       uint32
	F_iosize      uint32
	Pad_cgo_0     [4]byte
	F_blocks      uint64
	F_bfree       uint64
	F_bavail      int64
	F_files       uint64
	F_ffree       uint64
	F_favail      int64
	F_syncwrites  uint64
	F_syncreads   uint64
	F_asyncwrites uint64
	F_asyncreads  uint64
	F_fsid        Fsid
	F_namemax     uint32
	F_owner       uint32
	F_ctime       uint64
	F_fstypename  [16]int8
	F_mntonname   [90]int8
	F_mntfromname [90]int8
	F_mntfromspec [90]int8
	Pad_cgo_1     [2]byte
	Mount_info    [160]byte
}
type Diskstats struct {
	Name       [16]int8
	Busy       int32
	Pad_cgo_0  [4]byte
	Rxfer      uint64
	Wxfer      uint64
	Seek       uint64
	Rbytes     uint64
	Wbytes     uint64
	Attachtime Timeval
	Timestamp  Timeval
	Time       Timeval
}
type Fsid struct {
	Val [2]int32
}
type Timeval struct {
	Sec  int64
	Usec int64
}

type Diskstat struct{}
type Bintime struct{}
