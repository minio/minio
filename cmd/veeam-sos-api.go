// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"os"
	"strings"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/logger"
)

// From Veeam-SOSAPI_1.0_Document_v1.02d.pdf
// - SOSAPI Protocol Version
// - Model Name of the vendor plus version for statistical analysis.
// - List of Smart Object Storage protocol capabilities supported by the server.
// Currently, there are three capabilities supported:
//   - Capacity Reporting
//   - Backup data locality for upload sessions (Veeam Smart Entity)
//   - Handover of IAM & STS Endpoints instead of manual definition in Veeam Backup & Replication. This allows Veeam
//     Agents to directly backup to object storage.
//
// An object storage system can implement one, multiple, or all functions.
//
//   - Optional (mandatory if <IAMSTS> is true): Set Endpoints for IAM and STS processing.
//
//   - Optional: Set server preferences for Backup & Replication parallel sessions, batch size of deletes, and block sizes (before
//     compression). This is an optional area; by default, there should be no <SystemRecommendations> section in the
//     system.xml. Vendors can work with Veeam Product Management and the Alliances team on getting approval to integrate
//     specific system recommendations based on current support case statistics and storage performance possibilities.
//     Vendors might change the settings based on the configuration and scale out of the solution (more storage nodes =>
//     higher task limit).
//
//     <S3ConcurrentTaskLimit>
//
//   - Defines how many S3 operations are executed parallel within one Repository Task Slot (and within one backup object
//     that gets offloaded). The same registry key setting overwrites the storage-defined setting.
//     Optional value, default 64, range: 1-unlimited
//
//   - <S3MultiObjectDeleteLimit>
//     Some of the Veeam products use Multi Delete operations. This setting can reduce how many objects are included in one
//     multi-delete operation. The same registry key setting overwrites the storage-defined setting.
//     Optional value, default 1000, range: 1-unlimited (S3 standard maximum is 1000 and should not be set higher)
//
//   - <StorageConcurrentTasksLimit>
//     Setting reduces the parallel Repository Task slots that offload or write data to object storage. The same user interface
//     setting overwrites the storage-defined setting.
//     Optional value, default 0, range: 0-unlimited (0 equals unlimited, which means the maximum configured repository task
//     slots are used for object offloading or writing)
//
//   - <KbBlockSize>
//     Veeam Block Size for backup and restore processing before compression is applied. The higher the block size, the more
//     backup space is needed for incremental backups. Larger block sizes also mean less performance for random read restore
//     methods like Instant Restore, File Level Recovery, and Database/Application restores. Veeam recommends that vendors
//     optimize the storage system for the default value of 1MB minus compression object sizes. The setting simultaneously
//     affects read from source, block, file, dedup, and object storage backup targets for a specific Veeam Job. When customers
//     create a new backup job and select the object storage or a SOBR as a backup target with this setting, the job default
//     setting will be set to this value. This setting will be only applied to newly created jobs (manual changes with Active Full
//     processing possible from the customer side).
//     Optional value, default 1024, allowed values 256,512,1024,4096,8192, value defined in KB size.
//
// - The object should be present in all buckets accessed by Veeam products that want to leverage the SOSAPI functionality.
//
// - The current protocol version is 1.0.
type apiEndpoints struct {
	IAMEndpoint string `xml:"IAMEndpoint"`
	STSEndpoint string `xml:"STSEndpoint"`
}

// globalVeeamForceSC is set by the environment variable _MINIO_VEEAM_FORCE_SC
// This will override the storage class returned by the storage backend if it is non-standard
// and we detect a Veeam client by checking the User Agent.
var globalVeeamForceSC = os.Getenv("_MINIO_VEEAM_FORCE_SC")

type systemInfo struct {
	XMLName              xml.Name `xml:"SystemInfo" json:"-"`
	ProtocolVersion      string   `xml:"ProtocolVersion"`
	ModelName            string   `xml:"ModelName"`
	ProtocolCapabilities struct {
		CapacityInfo   bool `xml:"CapacityInfo"`
		UploadSessions bool `xml:"UploadSessions"`
		IAMSTS         bool `xml:"IAMSTS"`
	} `mxl:"ProtocolCapabilities"`
	APIEndpoints          *apiEndpoints `xml:"APIEndpoints,omitempty"`
	SystemRecommendations struct {
		S3ConcurrentTaskLimit    int `xml:"S3ConcurrentTaskLimit,omitempty"`
		S3MultiObjectDeleteLimit int `xml:"S3MultiObjectDeleteLimit,omitempty"`
		StorageCurrentTaskLimit  int `xml:"StorageCurrentTaskLimit,omitempty"`
		KBBlockSize              int `xml:"KbBlockSize"`
	} `xml:"SystemRecommendations"`
}

// This optional functionality allows vendors to report space information to Veeam products, and Veeam will make placement
// decisions based on this information. For example, Veeam Backup & Replication has a Scale-out-Backup-Repository feature where
// multiple buckets can be used together. The placement logic for additional backup files is based on available space. Other values
// will augment the Veeam user interface and statistics, including free space warnings.
type capacityInfo struct {
	XMLName   xml.Name `xml:"CapacityInfo" json:"-"`
	Capacity  int64    `xml:"Capacity"`
	Available int64    `xml:"Available"`
	Used      int64    `xml:"Used"`
}

const (
	systemXMLObject   = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/system.xml"
	capacityXMLObject = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c/capacity.xml"
	veeamAgentSubstr  = "APN/1.0 Veeam/1.0"
)

func isVeeamSOSAPIObject(object string) bool {
	switch object {
	case systemXMLObject, capacityXMLObject:
		return true
	default:
		return false
	}
}

// isVeeamClient - returns true if the request is from Veeam client.
func isVeeamClient(ctx context.Context) bool {
	ri := logger.GetReqInfo(ctx)
	return ri != nil && strings.Contains(ri.UserAgent, veeamAgentSubstr)
}

func veeamSOSAPIHeadObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	gr, err := veeamSOSAPIGetObject(ctx, bucket, object, nil, opts)
	if gr != nil {
		gr.Close()
		return gr.ObjInfo, nil
	}
	return ObjectInfo{}, err
}

func veeamSOSAPIGetObject(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, opts ObjectOptions) (gr *GetObjectReader, err error) {
	var buf []byte
	switch object {
	case systemXMLObject:
		si := systemInfo{
			ProtocolVersion: `"1.0"`,
			ModelName:       "\"MinIO " + ReleaseTag + "\"",
		}
		si.ProtocolCapabilities.CapacityInfo = true

		// Default recommended block size with MinIO
		si.SystemRecommendations.KBBlockSize = 4096

		buf = encodeResponse(&si)
	case capacityXMLObject:
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			return nil, errServerNotInitialized
		}

		q, _ := globalBucketQuotaSys.Get(ctx, bucket)
		binfo := globalBucketQuotaSys.GetBucketUsageInfo(ctx, bucket)

		ci := capacityInfo{
			Used: int64(binfo.Size),
		}

		var quotaSize int64
		if q != nil && q.Type == madmin.HardQuota {
			if q.Size > 0 {
				quotaSize = int64(q.Size)
			} else if q.Quota > 0 {
				quotaSize = int64(q.Quota)
			}
		}

		if quotaSize == 0 {
			info := objAPI.StorageInfo(ctx, true)
			info.Backend = objAPI.BackendInfo()

			ci.Capacity = int64(GetTotalUsableCapacity(info.Disks, info))
		} else {
			ci.Capacity = quotaSize
		}
		ci.Available = ci.Capacity - ci.Used

		buf = encodeResponse(&ci)
	default:
		return nil, errFileNotFound
	}

	etag := getMD5Hash(buf)
	r := bytes.NewReader(buf)

	off, length := int64(0), r.Size()
	if rs != nil {
		off, length, err = rs.GetOffsetLength(r.Size())
		if err != nil {
			return nil, err
		}
	}
	r.Seek(off, io.SeekStart)

	return NewGetObjectReaderFromReader(io.LimitReader(r, length), ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        r.Size(),
		IsLatest:    true,
		ContentType: string(mimeXML),
		NumVersions: 1,
		ETag:        etag,
		ModTime:     UTCNow(),
	}, opts)
}
