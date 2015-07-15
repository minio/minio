package donut

import (
	"fmt"

	"github.com/minio/minio/pkg/iodine"
)

// Heal heal an existing donut
func (donut API) Heal() error {
	missingDisks := make(map[int]struct{})
	for _, node := range donut.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for i, disk := range disks {
			dirs, err := disk.ListDir(donut.config.DonutName)
			if err != nil {
				missingDisks[i] = struct{}{}
			}
			fmt.Println(dirs)
		}
	}
	return nil
}
