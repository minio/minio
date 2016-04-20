package main

// containerAPI provides methods for controller API operations.
type controllerAPI struct {
	storage StorageAPI
}

// newController initialize new controller.
func newController(storage StorageAPI) controllerAPI {
	return controllerAPI{storage}
}

// HealObjectInfo - heal object info.
type HealObjectInfo struct {
	Bucket string `json:"bucket"`
	Object string `json:"object"`
	Status bool   `json:"healStatus"`
	Err    error  `json:"error"`
}

// HealObject heals an object.
func (c controllerAPI) HealObject(bucket string, prefix string, recursive bool, doneCh <-chan struct{}) <-chan HealObjectInfo {
	var healObjCh = make(chan HealObjectInfo)
	// Verify if bucket is valid.
	if !IsValidBucketName(bucket) {
		defer close(healObjCh)
		healObjCh <- HealObjectInfo{
			Err: BucketNameInvalid{Bucket: bucket},
		}
		return healObjCh
	}
	if !IsValidObjectPrefix(prefix) {
		defer close(healObjCh)
		healObjCh <- HealObjectInfo{
			Err: ObjectNameInvalid{Bucket: bucket, Object: prefix},
		}
		return healObjCh
	}
	// Initiate list objects goroutine here.
	go func(healObjCh chan<- HealObjectInfo) {
		defer close(healObjCh)
		var markerPath string
		for {
			fileInfos, eof, e := c.storage.ListFiles(bucket, prefix, markerPath, recursive, 1000)
			if e != nil {
				healObjCh <- HealObjectInfo{
					Err: e,
				}
				return
			}
			for _, fileInfo := range fileInfos {
				objName := fileInfo.Name
				if e = c.storage.HealFile(bucket, objName); e != nil {
					healObjCh <- HealObjectInfo{
						Err: e,
					}
					continue
				}
				healObjCh <- HealObjectInfo{
					Bucket: bucket,
					Object: objName,
					Status: true,
					Err:    nil,
				}
			}
			if eof {
				break
			}
			// MarkerPath to get the next set of files.
			markerPath = fileInfos[len(fileInfos)-1].Name
		}
	}(healObjCh)
	return healObjCh
}
