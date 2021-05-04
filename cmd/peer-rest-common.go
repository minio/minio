// Copyright (c) 2015-2021 MinIO, Inc.
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
	peerRESTVersion       = "v15" // Add LoadTransitionTierConfig
	peerRESTVersionPrefix = SlashSeparator + peerRESTVersion
	peerRESTPrefix        = minioReservedBucketPath + "/peer"
	peerRESTPath          = peerRESTPrefix + peerRESTVersionPrefix
)

const (
	peerRESTMethodHealth                   = "/health"
	peerRESTMethodServerInfo               = "/serverinfo"
	peerRESTMethodDriveInfo                = "/driveinfo"
	peerRESTMethodNetInfo                  = "/netinfo"
	peerRESTMethodCPUInfo                  = "/cpuinfo"
	peerRESTMethodDiskHwInfo               = "/diskhwinfo"
	peerRESTMethodOsInfo                   = "/osinfo"
	peerRESTMethodMemInfo                  = "/meminfo"
	peerRESTMethodProcInfo                 = "/procinfo"
	peerRESTMethodDispatchNetInfo          = "/dispatchnetinfo"
	peerRESTMethodDeleteBucketMetadata     = "/deletebucketmetadata"
	peerRESTMethodLoadBucketMetadata       = "/loadbucketmetadata"
	peerRESTMethodGetBucketStats           = "/getbucketstats"
	peerRESTMethodServerUpdate             = "/serverupdate"
	peerRESTMethodSignalService            = "/signalservice"
	peerRESTMethodBackgroundHealStatus     = "/backgroundhealstatus"
	peerRESTMethodGetLocks                 = "/getlocks"
	peerRESTMethodLoadUser                 = "/loaduser"
	peerRESTMethodLoadServiceAccount       = "/loadserviceaccount"
	peerRESTMethodDeleteUser               = "/deleteuser"
	peerRESTMethodDeleteServiceAccount     = "/deleteserviceaccount"
	peerRESTMethodLoadPolicy               = "/loadpolicy"
	peerRESTMethodLoadPolicyMapping        = "/loadpolicymapping"
	peerRESTMethodDeletePolicy             = "/deletepolicy"
	peerRESTMethodLoadGroup                = "/loadgroup"
	peerRESTMethodStartProfiling           = "/startprofiling"
	peerRESTMethodDownloadProfilingData    = "/downloadprofilingdata"
	peerRESTMethodCycleBloom               = "/cyclebloom"
	peerRESTMethodTrace                    = "/trace"
	peerRESTMethodListen                   = "/listen"
	peerRESTMethodLog                      = "/log"
	peerRESTMethodGetLocalDiskIDs          = "/getlocaldiskids"
	peerRESTMethodGetBandwidth             = "/bandwidth"
	peerRESTMethodGetMetacacheListing      = "/getmetacache"
	peerRESTMethodUpdateMetacacheListing   = "/updatemetacache"
	peerRESTMethodGetPeerMetrics           = "/peermetrics"
	peerRESTMethodLoadTransitionTierConfig = "/loadtransitiontierconfig"
)

const (
	peerRESTBucket         = "bucket"
	peerRESTBuckets        = "buckets"
	peerRESTUser           = "user"
	peerRESTGroup          = "group"
	peerRESTUserTemp       = "user-temp"
	peerRESTPolicy         = "policy"
	peerRESTUserOrGroup    = "user-or-group"
	peerRESTIsGroup        = "is-group"
	peerRESTSignal         = "signal"
	peerRESTProfiler       = "profiler"
	peerRESTTraceErr       = "err"
	peerRESTTraceInternal  = "internal"
	peerRESTTraceStorage   = "storage"
	peerRESTTraceS3        = "s3"
	peerRESTTraceOS        = "os"
	peerRESTTraceThreshold = "threshold"

	peerRESTListenBucket = "bucket"
	peerRESTListenPrefix = "prefix"
	peerRESTListenSuffix = "suffix"
	peerRESTListenEvents = "events"
)
