// +build ignore
// Hand writing: _Ctype_struct___0

/*
Input to cgo -godefs.
*/

package disk

/*
#include <sys/types.h>
#include <sys/disk.h>
#include <sys/mount.h>

enum {
	sizeofPtr = sizeof(void*),
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
	MNT_NODEV       = 0x00000010 /* don't interpret special files */
	MNT_ASYNC       = 0x00000040 /* filesystem written asynchronously */

	MNT_WAIT   = 1 /* synchronously wait for I/O to complete */
	MNT_NOWAIT = 2 /* start all I/O, but do not wait for it */
	MNT_LAZY   = 3 /* push data not written by filesystem syncer */
)

const (
	sizeOfDiskstats = C.sizeof_struct_diskstats
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
type Diskstats C.struct_diskstats
type Fsid C.fsid_t
type Timeval C.struct_timeval

type Diskstat C.struct_diskstat
type Bintime C.struct_bintime
