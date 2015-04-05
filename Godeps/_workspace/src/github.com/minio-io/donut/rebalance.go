package donut

import (
	"fmt"
	"os"
	"strings"
)

func (d donut) Rebalance() error {
	var totalOffSetLength int
	var newDisks []Disk
	var existingDirs []os.FileInfo
	for _, node := range d.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return err
		}
		totalOffSetLength = len(disks)
		fmt.Println(totalOffSetLength)
		for _, disk := range disks {
			dirs, err := disk.ListDir(d.name)
			if err != nil {
				return err
			}
			if len(dirs) == 0 {
				newDisks = append(newDisks, disk)
			}
			existingDirs = append(existingDirs, dirs...)
		}
	}
	for _, dir := range existingDirs {
		splits := strings.Split(dir.Name(), "$")
		bucketName, segment, offset := splits[0], splits[1], splits[2]
		fmt.Println(bucketName, segment, offset)
	}
	return nil
}
