// +build ignore

/*
Input to cgo -godefs.
*/

package mem

/*
#include <sys/types.h>
#include <sys/mount.h>
#include <sys/sysctl.h>
#include <uvm/uvmexp.h>

*/
import "C"

// Machine characteristics; for internal use.

const (
	CTLVm         = 2
	CTLVfs        = 10
	VmUvmexp      = 4 // get uvmexp
	VfsGeneric    = 0
	VfsBcacheStat = 3
)

const (
	sizeOfUvmexp      = C.sizeof_struct_uvmexp
	sizeOfBcachestats = C.sizeof_struct_bcachestats
)

type Uvmexp C.struct_uvmexp
type Bcachestats C.struct_bcachestats
