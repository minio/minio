/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rpc

import (
	"net/http"
	"os"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/utils/scsi"
)

// DiskInfoService disk info service
type DiskInfoService struct{}

// DiskInfoReply disk info reply for disk info service
type DiskInfoReply struct {
	Hostname       string                     `json:"hostname"`
	Disks          []string                   `json:"disks"`
	DiskAttributes map[string]scsi.Attributes `json:"disk-attrs"`
}

func setDiskInfoReply(sis *DiskInfoReply) error {
	var err error
	sis.Hostname, err = os.Hostname()
	if err != nil {
		return iodine.New(err, nil)
	}
	disks, err := scsi.GetDisks()
	if err != nil {
		return iodine.New(err, nil)
	}
	sis.DiskAttributes = make(map[string]scsi.Attributes)
	for k, v := range disks {
		sis.Disks = append(sis.Disks, k)
		sis.DiskAttributes[k] = v
	}
	return nil
}

// Get method
func (s *DiskInfoService) Get(r *http.Request, args *Args, reply *DiskInfoReply) error {
	return setDiskInfoReply(reply)
}
