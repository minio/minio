/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package cmd

const (
	peerRESTVersion       = "v6"
	peerRESTVersionPrefix = SlashSeparator + peerRESTVersion
	peerRESTPrefix        = minioReservedBucketPath + "/peer"
	peerRESTPath          = peerRESTPrefix + peerRESTVersionPrefix
)

const (
	peerRESTMethodNetReadPerfInfo              = "/netreadperfinfo"
	peerRESTMethodCollectNetPerfInfo           = "/collectnetperfinfo"
	peerRESTMethodServerInfo                   = "/serverinfo"
	peerRESTMethodCPULoadInfo                  = "/cpuloadinfo"
	peerRESTMethodMemUsageInfo                 = "/memusageinfo"
	peerRESTMethodDrivePerfInfo                = "/driveperfinfo"
	peerRESTMethodDeleteBucket                 = "/deletebucket"
	peerRESTMethodServerUpdate                 = "/serverupdate"
	peerRESTMethodSignalService                = "/signalservice"
	peerRESTMethodBackgroundHealStatus         = "/backgroundhealstatus"
	peerRESTMethodBackgroundOpsStatus          = "/backgroundopsstatus"
	peerRESTMethodGetLocks                     = "/getlocks"
	peerRESTMethodBucketPolicyRemove           = "/removebucketpolicy"
	peerRESTMethodLoadUser                     = "/loaduser"
	peerRESTMethodDeleteUser                   = "/deleteuser"
	peerRESTMethodLoadPolicy                   = "/loadpolicy"
	peerRESTMethodLoadPolicyMapping            = "/loadpolicymapping"
	peerRESTMethodDeletePolicy                 = "/deletepolicy"
	peerRESTMethodLoadUsers                    = "/loadusers"
	peerRESTMethodLoadGroup                    = "/loadgroup"
	peerRESTMethodStartProfiling               = "/startprofiling"
	peerRESTMethodDownloadProfilingData        = "/downloadprofilingdata"
	peerRESTMethodBucketPolicySet              = "/setbucketpolicy"
	peerRESTMethodBucketNotificationPut        = "/putbucketnotification"
	peerRESTMethodBucketNotificationListen     = "/listenbucketnotification"
	peerRESTMethodReloadFormat                 = "/reloadformat"
	peerRESTMethodTargetExists                 = "/targetexists"
	peerRESTMethodSendEvent                    = "/sendevent"
	peerRESTMethodTrace                        = "/trace"
	peerRESTMethodListen                       = "/listen"
	peerRESTMethodBucketLifecycleSet           = "/setbucketlifecycle"
	peerRESTMethodBucketLifecycleRemove        = "/removebucketlifecycle"
	peerRESTMethodLog                          = "/log"
	peerRESTMethodHardwareCPUInfo              = "/cpuhardwareinfo"
	peerRESTMethodHardwareNetworkInfo          = "/networkhardwareinfo"
	peerRESTMethodPutBucketObjectLockConfig    = "/putbucketobjectlockconfig"
	peerRESTMethodBucketObjectLockConfigRemove = "/removebucketobjectlockconfig"
)

const (
	peerRESTNetPerfSize   = "netperfsize"
	peerRESTDrivePerfSize = "driveperfsize"
	peerRESTBucket        = "bucket"
	peerRESTUser          = "user"
	peerRESTGroup         = "group"
	peerRESTUserTemp      = "user-temp"
	peerRESTPolicy        = "policy"
	peerRESTUserOrGroup   = "user-or-group"
	peerRESTIsGroup       = "is-group"
	peerRESTUpdateURL     = "updateURL"
	peerRESTSha256Hex     = "sha256Hex"
	peerRESTLatestRelease = "latestReleaseTime"
	peerRESTSignal        = "signal"
	peerRESTProfiler      = "profiler"
	peerRESTDryRun        = "dry-run"
	peerRESTTraceAll      = "all"
	peerRESTTraceErr      = "err"
)
