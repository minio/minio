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

const (
	peerRESTVersion       = "v38" // Convert RPC calls
	peerRESTVersionPrefix = SlashSeparator + peerRESTVersion
	peerRESTPrefix        = minioReservedBucketPath + "/peer"
	peerRESTPath          = peerRESTPrefix + peerRESTVersionPrefix
)

const (
	peerRESTMethodHealth                      = "/health"
	peerRESTMethodVerifyBinary                = "/verifybinary"
	peerRESTMethodCommitBinary                = "/commitbinary"
	peerRESTMethodSignalService               = "/signalservice"
	peerRESTMethodBackgroundHealStatus        = "/backgroundhealstatus"
	peerRESTMethodGetLocks                    = "/getlocks"
	peerRESTMethodStartProfiling              = "/startprofiling"
	peerRESTMethodDownloadProfilingData       = "/downloadprofilingdata"
	peerRESTMethodGetBandwidth                = "/bandwidth"
	peerRESTMethodSpeedTest                   = "/speedtest"
	peerRESTMethodDriveSpeedTest              = "/drivespeedtest"
	peerRESTMethodReloadSiteReplicationConfig = "/reloadsitereplicationconfig"
	peerRESTMethodGetLastDayTierStats         = "/getlastdaytierstats"
	peerRESTMethodDevNull                     = "/devnull"
	peerRESTMethodNetperf                     = "/netperf"
	peerRESTMethodGetReplicationMRF           = "/getreplicationmrf"
)

const (
	peerRESTBucket         = "bucket"
	peerRESTBuckets        = "buckets"
	peerRESTUser           = "user"
	peerRESTGroup          = "group"
	peerRESTUserTemp       = "user-temp"
	peerRESTPolicy         = "policy"
	peerRESTUserOrGroup    = "user-or-group"
	peerRESTUserType       = "user-type"
	peerRESTIsGroup        = "is-group"
	peerRESTSignal         = "signal"
	peerRESTSubSys         = "sub-sys"
	peerRESTProfiler       = "profiler"
	peerRESTSize           = "size"
	peerRESTConcurrent     = "concurrent"
	peerRESTDuration       = "duration"
	peerRESTStorageClass   = "storage-class"
	peerRESTEnableSha256   = "enableSha256"
	peerRESTMetricsTypes   = "types"
	peerRESTDisk           = "disk"
	peerRESTHost           = "host"
	peerRESTJobID          = "job-id"
	peerRESTDepID          = "depID"
	peerRESTStartRebalance = "start-rebalance"
	peerRESTMetrics        = "metrics"
	peerRESTDryRun         = "dry-run"

	peerRESTURL         = "url"
	peerRESTSha256Sum   = "sha256sum"
	peerRESTReleaseInfo = "releaseinfo"

	peerRESTListenBucket = "bucket"
	peerRESTListenPrefix = "prefix"
	peerRESTListenSuffix = "suffix"
	peerRESTListenEvents = "events"
	peerRESTLogMask      = "log-mask"
)
