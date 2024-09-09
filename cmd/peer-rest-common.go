// Copyright (c) 2015-2024 MinIO, Inc.
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

import "time"

const (
	peerRESTVersion       = "v39" // add more flags to speedtest API
	peerRESTVersionPrefix = SlashSeparator + peerRESTVersion
	peerRESTPrefix        = minioReservedBucketPath + "/peer"
	peerRESTPath          = peerRESTPrefix + peerRESTVersionPrefix
)

const (
	peerRESTMethodHealth                = "/health"
	peerRESTMethodVerifyBinary          = "/verifybinary"
	peerRESTMethodCommitBinary          = "/commitbinary"
	peerRESTMethodStartProfiling        = "/startprofiling"
	peerRESTMethodDownloadProfilingData = "/downloadprofilingdata"
	peerRESTMethodSpeedTest             = "/speedtest"
	peerRESTMethodDriveSpeedTest        = "/drivespeedtest"
	peerRESTMethodDevNull               = "/devnull"
	peerRESTMethodNetperf               = "/netperf"
	peerRESTMethodGetReplicationMRF     = "/getreplicationmrf"
)

const (
	peerRESTBucket          = "bucket"
	peerRESTBuckets         = "buckets"
	peerRESTUser            = "user"
	peerRESTGroup           = "group"
	peerRESTUserTemp        = "user-temp"
	peerRESTPolicy          = "policy"
	peerRESTUserOrGroup     = "user-or-group"
	peerRESTUserType        = "user-type"
	peerRESTIsGroup         = "is-group"
	peerRESTSignal          = "signal"
	peerRESTSubSys          = "sub-sys"
	peerRESTProfiler        = "profiler"
	peerRESTSize            = "size"
	peerRESTConcurrent      = "concurrent"
	peerRESTDuration        = "duration"
	peerRESTStorageClass    = "storage-class"
	peerRESTEnableSha256    = "enableSha256"
	peerRESTEnableMultipart = "enableMultipart"
	peerRESTAccessKey       = "access-key"
	peerRESTMetricsTypes    = "types"
	peerRESTDisk            = "disk"
	peerRESTHost            = "host"
	peerRESTJobID           = "job-id"
	peerRESTDepID           = "depID"
	peerRESTStartRebalance  = "start-rebalance"
	peerRESTMetrics         = "metrics"
	peerRESTDryRun          = "dry-run"
	peerRESTUploadID        = "up-id"

	peerRESTURL         = "url"
	peerRESTSha256Sum   = "sha256sum"
	peerRESTReleaseInfo = "releaseinfo"
	peerRESTExecAt      = "exec-at"

	peerRESTListenBucket = "bucket"
	peerRESTListenPrefix = "prefix"
	peerRESTListenSuffix = "suffix"
	peerRESTListenEvents = "events"
	peerRESTLogMask      = "log-mask"
)

const restartUpdateDelay = 250 * time.Millisecond
