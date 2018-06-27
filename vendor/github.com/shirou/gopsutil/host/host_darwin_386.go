// Created by cgo -godefs - DO NOT EDIT
// cgo -godefs types_darwin.go

package host

type Utmpx struct {
	User      [256]int8
	ID        [4]int8
	Line      [32]int8
	Pid       int32
	Type      int16
	Pad_cgo_0 [6]byte
	Tv        Timeval
	Host      [256]int8
	Pad       [16]uint32
}
type Timeval struct {
	Sec int32
}
