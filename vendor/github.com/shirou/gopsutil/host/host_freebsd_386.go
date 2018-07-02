// Created by cgo -godefs - DO NOT EDIT
// cgo -godefs types_freebsd.go

package host

const (
	sizeofPtr      = 0x4
	sizeofShort    = 0x2
	sizeofInt      = 0x4
	sizeofLong     = 0x4
	sizeofLongLong = 0x8
	sizeOfUtmpx    = 197 // TODO why should 197
)

type (
	_C_short     int16
	_C_int       int32
	_C_long      int32
	_C_long_long int64
)

type Utmp struct {
	Line [8]int8
	Name [16]int8
	Host [16]int8
	Time int32
}

type Utmpx struct {
	Type int16
	Tv   Timeval
	Id   [8]int8
	Pid  int32
	User [32]int8
	Line [16]int8
	Host [125]int8
	//      X__ut_spare     [64]int8
}

type Timeval struct {
	Sec  [4]byte
	Usec [3]byte
}
