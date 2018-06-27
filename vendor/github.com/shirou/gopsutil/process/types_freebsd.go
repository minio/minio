// +build ignore

// We still need editing by hands.
// go tool cgo -godefs types_freebsd.go | sed 's/\*int64/int64/' | sed 's/\*byte/int64/'  > process_freebsd_amd64.go

/*
Input to cgo -godefs.
*/

// +godefs map struct_pargs int64 /* pargs */
// +godefs map struct_proc int64 /* proc */
// +godefs map struct_user int64 /* user */
// +godefs map struct_vnode int64 /* vnode */
// +godefs map struct_vnode int64 /* vnode */
// +godefs map struct_filedesc int64 /* filedesc */
// +godefs map struct_vmspace int64 /* vmspace */
// +godefs map struct_pcb int64 /* pcb */
// +godefs map struct_thread int64 /* thread */
// +godefs map struct___sigset [16]byte /* sigset */

package process

/*
#include <sys/types.h>
#include <sys/user.h>

enum {
	sizeofPtr = sizeof(void*),
};


*/
import "C"

// Machine characteristics; for internal use.

const (
	CTLKern          = 1  // "high kernel": proc, limits
	KernProc         = 14 // struct: process entries
	KernProcPID      = 1  // by process id
	KernProcProc     = 8  // only return procs
	KernProcPathname = 12 // path to executable
	KernProcArgs     = 7  // get/set arguments/proctitle
)

const (
	sizeofPtr      = C.sizeofPtr
	sizeofShort    = C.sizeof_short
	sizeofInt      = C.sizeof_int
	sizeofLong     = C.sizeof_long
	sizeofLongLong = C.sizeof_longlong
)

const (
	sizeOfKinfoVmentry = C.sizeof_struct_kinfo_vmentry
	sizeOfKinfoProc    = C.sizeof_struct_kinfo_proc
)

// from sys/proc.h
const (
	SIDL   = 1 /* Process being created by fork. */
	SRUN   = 2 /* Currently runnable. */
	SSLEEP = 3 /* Sleeping on an address. */
	SSTOP  = 4 /* Process debugging or suspension. */
	SZOMB  = 5 /* Awaiting collection by parent. */
	SWAIT  = 6 /* Waiting for interrupt. */
	SLOCK  = 7 /* Blocked on a lock. */
)

// Basic types

type (
	_C_short     C.short
	_C_int       C.int
	_C_long      C.long
	_C_long_long C.longlong
)

// Time

type Timespec C.struct_timespec

type Timeval C.struct_timeval

// Processes

type Rusage C.struct_rusage

type Rlimit C.struct_rlimit

type KinfoProc C.struct_kinfo_proc

type Priority C.struct_priority

type KinfoVmentry C.struct_kinfo_vmentry
