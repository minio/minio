// +build ignore
// Hand writing: _Ctype_struct___0

/*
Input to cgo -godefs.

*/

package disk

/*
#include <sys/types.h>
#include <sys/mount.h>
#include <devstat.h>

enum {
	sizeofPtr = sizeof(void*),
};

// because statinfo has long double snap_time, redefine with changing long long
struct statinfo2 {
        long            cp_time[CPUSTATES];
        long            tk_nin;
        long            tk_nout;
        struct devinfo  *dinfo;
        long long       snap_time;
};
*/
import "C"

// Machine characteristics; for internal use.

const (
	sizeofPtr        = C.sizeofPtr
	sizeofShort      = C.sizeof_short
	sizeofInt        = C.sizeof_int
	sizeofLong       = C.sizeof_long
	sizeofLongLong   = C.sizeof_longlong
	sizeofLongDouble = C.sizeof_longlong

	DEVSTAT_NO_DATA = 0x00
	DEVSTAT_READ    = 0x01
	DEVSTAT_WRITE   = 0x02
	DEVSTAT_FREE    = 0x03

	// from sys/mount.h
	MNT_RDONLY      = 0x00000001 /* read only filesystem */
	MNT_SYNCHRONOUS = 0x00000002 /* filesystem written synchronously */
	MNT_NOEXEC      = 0x00000004 /* can't exec from filesystem */
	MNT_NOSUID      = 0x00000008 /* don't honor setuid bits on fs */
	MNT_UNION       = 0x00000020 /* union with underlying filesystem */
	MNT_ASYNC       = 0x00000040 /* filesystem written asynchronously */
	MNT_SUIDDIR     = 0x00100000 /* special handling of SUID on dirs */
	MNT_SOFTDEP     = 0x00200000 /* soft updates being done */
	MNT_NOSYMFOLLOW = 0x00400000 /* do not follow symlinks */
	MNT_GJOURNAL    = 0x02000000 /* GEOM journal support enabled */
	MNT_MULTILABEL  = 0x04000000 /* MAC support for individual objects */
	MNT_ACLS        = 0x08000000 /* ACL support enabled */
	MNT_NOATIME     = 0x10000000 /* disable update of file access time */
	MNT_NOCLUSTERR  = 0x40000000 /* disable cluster read */
	MNT_NOCLUSTERW  = 0x80000000 /* disable cluster write */
	MNT_NFS4ACLS    = 0x00000010

	MNT_WAIT    = 1 /* synchronously wait for I/O to complete */
	MNT_NOWAIT  = 2 /* start all I/O, but do not wait for it */
	MNT_LAZY    = 3 /* push data not written by filesystem syncer */
	MNT_SUSPEND = 4 /* Suspend file system after sync */
)

const (
	sizeOfDevstat = C.sizeof_struct_devstat
)

// Basic types

type (
	_C_short       C.short
	_C_int         C.int
	_C_long        C.long
	_C_long_long   C.longlong
	_C_long_double C.longlong
)

type Statfs C.struct_statfs
type Fsid C.struct_fsid

type Devstat C.struct_devstat
type Bintime C.struct_bintime
