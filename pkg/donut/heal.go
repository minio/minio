package donut

import "github.com/minio/minio/pkg/iodine"

type missingDisk struct {
	nodeNumber  int
	sliceNumber int
	bucketName  string
}

// Heal heal an existing donut
func (donut API) Heal() error {
	var missingDisks []missingDisk
	nodeNumber := 0
	for _, node := range donut.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return iodine.New(err, nil)
		}
		for i, disk := range disks {
			_, err := disk.ListDir(donut.config.DonutName)
			if err == nil {
				continue
			}
			missingDisk := missingDisk{
				nodeNumber:  nodeNumber,
				sliceNumber: i,
			}
			missingDisks = append(missingDisks, missingDisk)
		}
	}
	return nil
}
