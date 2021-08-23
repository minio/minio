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

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/klauspost/compress/gzhttp"
	"github.com/klauspost/compress/gzip"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
)

const (
	adminPathPrefix       = minioReservedBucketPath + "/admin"
	adminAPIVersion       = madmin.AdminAPIVersion
	adminAPIVersionPrefix = SlashSeparator + adminAPIVersion
)

// adminAPIHandlers provides HTTP handlers for MinIO admin API.
type adminAPIHandlers struct{}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(router *mux.Router, enableConfigOps bool) {

	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := router.PathPrefix(adminPathPrefix).Subrouter()

	/// Service operations

	adminVersions := []string{
		adminAPIVersionPrefix,
	}

	gz, err := gzhttp.NewWrapper(gzhttp.MinSize(1000), gzhttp.CompressionLevel(gzip.BestSpeed))
	if err != nil {
		// Static params, so this is very unlikely.
		logger.Fatal(err, "Unable to initialize server")
	}

	for _, adminVersion := range adminVersions {
		// Restart and stop MinIO service.
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/service").HandlerFunc(gz(httpTraceAll(adminAPI.ServiceHandler))).Queries("action", "{action:.*}").Name("AdminService")
		// Update MinIO servers.
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/update").HandlerFunc(gz(httpTraceAll(adminAPI.ServerUpdateHandler))).Queries("updateURL", "{updateURL:.*}").Name("AdminServiceUpdate")

		// Info operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/info").HandlerFunc(gz(httpTraceAll(adminAPI.ServerInfoHandler))).Name("AdminInfo")
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/inspect-data").HandlerFunc(httpTraceHdrs(adminAPI.InspectDataHandler)).Queries("volume", "{volume:.*}", "file", "{file:.*}").Name("AdminInspectData")

		// StorageInfo operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/storageinfo").HandlerFunc(gz(httpTraceAll(adminAPI.StorageInfoHandler))).Name("AdminStorageInfo")
		// DataUsageInfo operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/datausageinfo").HandlerFunc(gz(httpTraceAll(adminAPI.DataUsageInfoHandler))).Name("AdminDataUsageInfo")

		if globalIsDistErasure || globalIsErasure {
			/// Heal operations

			// Heal processing endpoint.
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/").HandlerFunc(gz(httpTraceAll(adminAPI.HealHandler))).Name("AdminHeal")
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/{bucket}").HandlerFunc(gz(httpTraceAll(adminAPI.HealHandler))).Name("AdminHealBucket")
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/{bucket}/{prefix:.*}").HandlerFunc(gz(httpTraceAll(adminAPI.HealHandler))).Name("AdminHealBucketPrefix")

			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/background-heal/status").HandlerFunc(gz(httpTraceAll(adminAPI.BackgroundHealStatusHandler))).Name("AdminBackgroundHealStatus")

			/// Health operations

		}

		// Profiling operations
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/profiling/start").HandlerFunc(gz(httpTraceAll(adminAPI.StartProfilingHandler))).
			Queries("profilerType", "{profilerType:.*}").Name("AdminProfilingStart")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/profiling/download").HandlerFunc(gz(httpTraceAll(adminAPI.DownloadProfilingHandler))).Name("AdminProfilingDownload")

		// Config KV operations.
		if enableConfigOps {
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/get-config-kv").HandlerFunc(gz(httpTraceHdrs(adminAPI.GetConfigKVHandler))).Queries("key", "{key:.*}").Name("AdminGetConfigKV")
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/set-config-kv").HandlerFunc(gz(httpTraceHdrs(adminAPI.SetConfigKVHandler))).Name("AdminSetConfigKV")
			adminRouter.Methods(http.MethodDelete).Path(adminVersion + "/del-config-kv").HandlerFunc(gz(httpTraceHdrs(adminAPI.DelConfigKVHandler))).Name("AdminDelConfigKV")
		}

		// Enable config help in all modes.
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/help-config-kv").HandlerFunc(gz(httpTraceAll(adminAPI.HelpConfigKVHandler))).Queries("subSys", "{subSys:.*}", "key", "{key:.*}").Name("AdminHelpConfigKV")

		// Config KV history operations.
		if enableConfigOps {
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-config-history-kv").HandlerFunc(gz(httpTraceAll(adminAPI.ListConfigHistoryKVHandler))).Queries("count", "{count:[0-9]+}").Name("AdminListConfigHistoryKV")
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/clear-config-history-kv").HandlerFunc(gz(httpTraceHdrs(adminAPI.ClearConfigHistoryKVHandler))).Queries("restoreId", "{restoreId:.*}").Name("AdminClearConfigHistoryKV")
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/restore-config-history-kv").HandlerFunc(gz(httpTraceHdrs(adminAPI.RestoreConfigHistoryKVHandler))).Queries("restoreId", "{restoreId:.*}").Name("AdminRestoreConfigHistoryKV")
		}

		/// Config import/export bulk operations
		if enableConfigOps {
			// Get config
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/config").HandlerFunc(gz(httpTraceHdrs(adminAPI.GetConfigHandler))).Name("AdminGetConfig")
			// Set config
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/config").HandlerFunc(gz(httpTraceHdrs(adminAPI.SetConfigHandler))).Name("AdminSetConfig")
		}

		// -- IAM APIs --

		// Add policy IAM
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/add-canned-policy").HandlerFunc(gz(httpTraceAll(adminAPI.AddCannedPolicy))).Queries("name", "{name:.*}").Name("AdminAddCannedPolicy")

		// Add user IAM
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/accountinfo").HandlerFunc(gz(httpTraceAll(adminAPI.AccountInfoHandler))).Name("AdminAccountInfo")

		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/add-user").HandlerFunc(gz(httpTraceHdrs(adminAPI.AddUser))).Queries("accessKey", "{accessKey:.*}").Name("AdminAddUser")

		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-user-status").HandlerFunc(gz(httpTraceHdrs(adminAPI.SetUserStatus))).Queries("accessKey", "{accessKey:.*}").Queries("status", "{status:.*}").Name("AdminSetUserStatus")

		// Service accounts ops
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/add-service-account").HandlerFunc(gz(httpTraceHdrs(adminAPI.AddServiceAccount))).Name("AdminAddServiceAccount")
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/update-service-account").HandlerFunc(gz(httpTraceHdrs(adminAPI.UpdateServiceAccount))).Queries("accessKey", "{accessKey:.*}").Name("AdminUpdateServiceAccount")
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-service-account").HandlerFunc(gz(httpTraceHdrs(adminAPI.InfoServiceAccount))).Queries("accessKey", "{accessKey:.*}").Name("AdminInfoServiceAccount")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-service-accounts").HandlerFunc(gz(httpTraceHdrs(adminAPI.ListServiceAccounts))).Name("AdminListServiceAccounts")
		adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/delete-service-account").HandlerFunc(gz(httpTraceHdrs(adminAPI.DeleteServiceAccount))).Queries("accessKey", "{accessKey:.*}").Name("AdminDeleteServiceAccount")

		// Info policy IAM latest
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-canned-policy").HandlerFunc(gz(httpTraceHdrs(adminAPI.InfoCannedPolicy))).Queries("name", "{name:.*}").Name("AdminInfoCannedPolicy")
		// List policies latest
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-canned-policies").HandlerFunc(gz(httpTraceHdrs(adminAPI.ListBucketPolicies))).Queries("bucket", "{bucket:.*}").Name("AdminListCannedPolicies")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-canned-policies").HandlerFunc(gz(httpTraceHdrs(adminAPI.ListCannedPolicies))).Name("AdminListCannedPolicies")

		// Remove policy IAM
		adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-canned-policy").HandlerFunc(gz(httpTraceHdrs(adminAPI.RemoveCannedPolicy))).Queries("name", "{name:.*}").Name("AdminRemoveCannedPolicy")

		// Set user or group policy
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-user-or-group-policy").
			HandlerFunc(gz(httpTraceHdrs(adminAPI.SetPolicyForUserOrGroup))).
			Queries("policyName", "{policyName:.*}", "userOrGroup", "{userOrGroup:.*}", "isGroup", "{isGroup:true|false}").Name("AdminSetUserOrGroupPolicy")

		// Remove user IAM
		adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-user").HandlerFunc(gz(httpTraceHdrs(adminAPI.RemoveUser))).Queries("accessKey", "{accessKey:.*}").Name("AdminRemoveUser")

		// List users
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-users").HandlerFunc(gz(httpTraceHdrs(adminAPI.ListBucketUsers))).Queries("bucket", "{bucket:.*}").Name("AdminListBucketUsers")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-users").HandlerFunc(gz(httpTraceHdrs(adminAPI.ListUsers))).Name("AdminListUsers")

		// User info
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/user-info").HandlerFunc(gz(httpTraceHdrs(adminAPI.GetUserInfo))).Queries("accessKey", "{accessKey:.*}").Name("AdminUserInfo")
		// Add/Remove members from group
		adminRouter.Methods(http.MethodPut).Path(adminVersion + "/update-group-members").HandlerFunc(gz(httpTraceHdrs(adminAPI.UpdateGroupMembers))).Name("AdminUpdateGroupMembers")

		// Get Group
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/group").HandlerFunc(gz(httpTraceHdrs(adminAPI.GetGroup))).Queries("group", "{group:.*}").Name("AdminGetGroup")

		// List Groups
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/groups").HandlerFunc(gz(httpTraceHdrs(adminAPI.ListGroups))).Name("AdminListGroups")

		// Set Group Status
		adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-group-status").HandlerFunc(gz(httpTraceHdrs(adminAPI.SetGroupStatus))).Queries("group", "{group:.*}").Queries("status", "{status:.*}").Name("AdminSetGroupStatus")

		if globalIsDistErasure || globalIsErasure {
			// GetBucketQuotaConfig
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/get-bucket-quota").HandlerFunc(
				gz(httpTraceHdrs(adminAPI.GetBucketQuotaConfigHandler))).Queries("bucket", "{bucket:.*}").Name("AdminGetBucketQuota")
			// PutBucketQuotaConfig
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-bucket-quota").HandlerFunc(
				gz(httpTraceHdrs(adminAPI.PutBucketQuotaConfigHandler))).Queries("bucket", "{bucket:.*}").Name("AdminSetBucketQuota")

			// Bucket replication operations
			// GetBucketTargetHandler
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-remote-targets").HandlerFunc(
				gz(httpTraceHdrs(adminAPI.ListRemoteTargetsHandler))).Queries("bucket", "{bucket:.*}", "type", "{type:.*}").Name("AdminListRemoteTargets")
			// SetRemoteTargetHandler
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-remote-target").HandlerFunc(
				gz(httpTraceHdrs(adminAPI.SetRemoteTargetHandler))).Queries("bucket", "{bucket:.*}").Name("AdminSetRemoteTarget")
			// RemoveRemoteTargetHandler
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-remote-target").HandlerFunc(
				gz(httpTraceHdrs(adminAPI.RemoveRemoteTargetHandler))).Queries("bucket", "{bucket:.*}", "arn", "{arn:.*}").Name("AdminRemoveRemoteTarget")

			// Remote Tier management operations
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/tier").HandlerFunc(gz(httpTraceHdrs(adminAPI.AddTierHandler))).Name("AdminAddTier")
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/tier/{tier}").HandlerFunc(gz(httpTraceHdrs(adminAPI.EditTierHandler))).Name("AdminEditTier")
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/tier").HandlerFunc(gz(httpTraceHdrs(adminAPI.ListTierHandler))).Name("AdminListTier")
		}

		if globalIsDistErasure {
			// Top locks
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/top/locks").HandlerFunc(gz(httpTraceHdrs(adminAPI.TopLocksHandler))).Name("AdminTopLocks")
			// Force unlocks paths
			adminRouter.Methods(http.MethodPost).Path(adminVersion+"/force-unlock").
				Queries("paths", "{paths:.*}").HandlerFunc(gz(httpTraceHdrs(adminAPI.ForceUnlockHandler))).Name("AdminForceUnlock")
		}

		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/speedtest").HandlerFunc(httpTraceHdrs(adminAPI.SpeedtestHandler)).Name("AdminSpeedTest")

		// HTTP Trace
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/trace").HandlerFunc(gz(http.HandlerFunc(adminAPI.TraceHandler))).Name("AdminTrace")

		// Console Logs
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/log").HandlerFunc(gz(httpTraceAll(adminAPI.ConsoleLogHandler))).Name("AdminConsoleLog")

		// -- KMS APIs --
		//
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/kms/status").HandlerFunc(gz(httpTraceAll(adminAPI.KMSStatusHandler))).Name("AdminKMSStatus")
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/kms/key/create").HandlerFunc(gz(httpTraceAll(adminAPI.KMSCreateKeyHandler))).Queries("key-id", "{key-id:.*}").Name("AdminKMSCreateKey")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/kms/key/status").HandlerFunc(gz(httpTraceAll(adminAPI.KMSKeyStatusHandler))).Name("AdminKMSKeyStatus")

		if !globalIsGateway {
			// Keep obdinfo for backward compatibility with mc
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/obdinfo").
				HandlerFunc(gz(httpTraceHdrs(adminAPI.HealthInfoHandler))).Name("AdminOBDInfo")
			// -- Health API --
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/healthinfo").
				HandlerFunc(gz(httpTraceHdrs(adminAPI.HealthInfoHandler))).Name("AdminHealthInfo")
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/bandwidth").
				HandlerFunc(gz(httpTraceHdrs(adminAPI.BandwidthMonitorHandler))).Name("AdminBandwidthMonitor")
		}
	}

	// If none of the routes match add default error handler routes
	adminRouter.NotFoundHandler = httpTraceAll(errorResponseHandler)
	adminRouter.MethodNotAllowedHandler = httpTraceAll(methodNotAllowedHandler("Admin"))
}
