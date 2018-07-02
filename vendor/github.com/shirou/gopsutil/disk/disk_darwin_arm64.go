// +build darwin
// +build arm64

package disk

const (
	MntWait    = 1
	MfsNameLen = 15 /* length of fs type name, not inc. nul */
	MNameLen   = 90 /* length of buffer for returned name */

	MFSTYPENAMELEN = 16 /* length of fs type name including null */
	MAXPATHLEN     = 1024
	MNAMELEN       = MAXPATHLEN

	SYS_GETFSSTAT64 = 347
)

type Fsid struct{ val [2]int32 } /* file system id type */
type uid_t int32

// sys/mount.h
const (
	MntReadOnly     = 0x00000001 /* read only filesystem */
	MntSynchronous  = 0x00000002 /* filesystem written synchronously */
	MntNoExec       = 0x00000004 /* can't exec from filesystem */
	MntNoSuid       = 0x00000008 /* don't honor setuid bits on fs */
	MntUnion        = 0x00000020 /* union with underlying filesystem */
	MntAsync        = 0x00000040 /* filesystem written asynchronously */
	MntSuidDir      = 0x00100000 /* special handling of SUID on dirs */
	MntSoftDep      = 0x00200000 /* soft updates being done */
	MntNoSymFollow  = 0x00400000 /* do not follow symlinks */
	MntGEOMJournal  = 0x02000000 /* GEOM journal support enabled */
	MntMultilabel   = 0x04000000 /* MAC support for individual objects */
	MntACLs         = 0x08000000 /* ACL support enabled */
	MntNoATime      = 0x10000000 /* disable update of file access time */
	MntClusterRead  = 0x40000000 /* disable cluster read */
	MntClusterWrite = 0x80000000 /* disable cluster write */
	MntNFS4ACLs     = 0x00000010
)

type Statfs struct {
	Bsize       uint32
	Iosize      int32
	Blocks      uint64
	Bfree       uint64
	Bavail      uint64
	Files       uint64
	Ffree       uint64
	Fsid        Fsid
	Owner       uint32
	Type        uint32
	Flags       uint32
	Fssubtype   uint32
	Fstypename  [16]int8
	Mntonname   [1024]int8
	Mntfromname [1024]int8
	Reserved    [8]uint32
}
