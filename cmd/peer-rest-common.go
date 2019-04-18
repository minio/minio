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

const peerRESTVersion = "v1"
const peerRESTPath = minioReservedBucketPath + "/peer/" + peerRESTVersion

const (
	peerRESTMethodServerInfo               = "serverinfo"
	peerRESTMethodCPULoadInfo              = "cpuloadinfo"
	peerRESTMethodMemUsageInfo             = "memusageinfo"
	peerRESTMethodDrivePerfInfo            = "driveperfinfo"
	peerRESTMethodDeleteBucket             = "deletebucket"
	peerRESTMethodSignalService            = "signalservice"
	peerRESTMethodGetLocks                 = "getlocks"
	peerRESTMethodBucketPolicyRemove       = "removebucketpolicy"
	peerRESTMethodLoadUsers                = "loadusers"
	peerRESTMethodStartProfiling           = "startprofiling"
	peerRESTMethodDownloadProfilingData    = "downloadprofilingdata"
	peerRESTMethodBucketPolicySet          = "setbucketpolicy"
	peerRESTMethodBucketNotificationPut    = "putbucketnotification"
	peerRESTMethodBucketNotificationListen = "listenbucketnotification"
	peerRESTMethodReloadFormat             = "reloadformat"
	peerRESTMethodTargetExists             = "targetexists"
	peerRESTMethodSendEvent                = "sendevent"
)

const (
	peerRESTBucket   = "bucket"
	peerRESTSignal   = "signal"
	peerRESTProfiler = "profiler"
	peerRESTDryRun   = "dry-run"
)
