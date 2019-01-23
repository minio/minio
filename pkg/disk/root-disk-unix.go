// +build !windows

package disk

import (
	"os"
	"syscall"
)

// IsRootDisk returns if diskPath belongs to root-disk, i.e the disk mounted at "/"
func IsRootDisk(diskPath string) (bool, error) {
	rootDisk := false
	diskInfo, err := os.Stat(diskPath)
	if err != nil {
		return false, err
	}
	rootInfo, err := os.Stat("/")
	if err != nil {
		return false, err
	}
	diskStat, diskStatOK := diskInfo.Sys().(*syscall.Stat_t)
	rootStat, rootStatOK := rootInfo.Sys().(*syscall.Stat_t)
	if diskStatOK && rootStatOK {
		if diskStat.Dev == rootStat.Dev {
			// Indicate if the disk path is on root disk. This is used to indicate the healing
			// process not to format the drive and end up healing it.
			rootDisk = true
		}
	}
	return rootDisk, nil
}
