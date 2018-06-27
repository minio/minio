// +build freebsd openbsd dragonfly netbsd

package memory

func sysTotalMemory() uint64 {
	s, err := sysctlUint64("hw.physmem")
	if err != nil {
		return 0
	}
	return s
}
