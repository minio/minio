// ATTENTION - FILE MANUAL FIXED AFTER CGO.
// Fixed line: Tv		_Ctype_struct_timeval -> Tv		UtTv
// Created by cgo -godefs, MANUAL FIXED
// cgo -godefs types_linux.go

package host

const (
	sizeofPtr      = 0x4
	sizeofShort    = 0x2
	sizeofInt      = 0x4
	sizeofLong     = 0x4
	sizeofLongLong = 0x8
	sizeOfUtmp     = 0x180
)

type (
	_C_short     int16
	_C_int       int32
	_C_long      int32
	_C_long_long int64
)

type utmp struct {
	Type      int16
	Pad_cgo_0 [2]byte
	Pid       int32
	Line      [32]int8
	ID        [4]int8
	User      [32]int8
	Host      [256]int8
	Exit      exit_status
	Session   int32
	Tv        UtTv
	Addr_v6   [4]int32
	X__unused [20]int8
}
type exit_status struct {
	Termination int16
	Exit        int16
}
type UtTv struct {
	Sec  int32
	Usec int32
}
