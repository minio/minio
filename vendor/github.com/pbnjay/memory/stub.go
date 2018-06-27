// +build !linux,!darwin,!windows,!freebsd,!dragonfly,!netbsd,!openbsd

package memory

func sysTotalMemory() uint64 {
	return 0
}
