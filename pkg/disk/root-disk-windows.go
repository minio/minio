// +build windows

package disk

// IsRootDisk returns if diskPath belongs to root-disk, i.e the disk mounted at "/"
func IsRootDisk(diskPath string) (bool, error) {
	// On windows a disk can never be mounted on a subpath.
	return false, nil
}
