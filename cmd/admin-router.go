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
	"github.com/minio/madmin-go"
)

const (
	adminPathPrefix       = minioReservedBucketPath + "/admin"
	adminAPIVersion       = madmin.AdminAPIVersion
	adminAPIVersionPrefix = SlashSeparator + adminAPIVersion
)

// adminAPIHandlers provides HTTP handlers for MinIO admin API.
type adminAPIHandlers struct{}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(router *mux.Router, enableConfigOps, enableIAMOps bool) {

	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := router.PathPrefix(adminPathPrefix).Subrouter()

	/// Service operations

	adminVersions := []string{
		adminAPIVersionPrefix,
	}

	for _, adminVersion := range adminVersions {
		// Restart and stop MinIO service.
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/service").HandlerFunc(httpTraceAll(adminAPI.ServiceHandler)).Queries("action", "{action:.*}")
		// Update MinIO servers.
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/update").HandlerFunc(httpTraceAll(adminAPI.ServerUpdateHandler)).Queries("updateURL", "{updateURL:.*}")

		// Info operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/info").HandlerFunc(httpTraceAll(adminAPI.ServerInfoHandler))

		// StorageInfo operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/storageinfo").HandlerFunc(httpTraceAll(adminAPI.StorageInfoHandler))
		// DataUsageInfo operations
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/datausageinfo").HandlerFunc(httpTraceAll(adminAPI.DataUsageInfoHandler))

		if globalIsDistErasure || globalIsErasure {
			/// Heal operations

			// Heal processing endpoint.
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/{bucket}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/heal/{bucket}/{prefix:.*}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))

			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/background-heal/status").HandlerFunc(httpTraceAll(adminAPI.BackgroundHealStatusHandler))

			/// Health operations

		}

		// Profiling operations
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/profiling/start").HandlerFunc(httpTraceAll(adminAPI.StartProfilingHandler)).
			Queries("profilerType", "{profilerType:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/profiling/download").HandlerFunc(httpTraceAll(adminAPI.DownloadProfilingHandler))

		// Config KV operations.
		if enableConfigOps {
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/get-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigKVHandler)).Queries("key", "{key:.*}")
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/set-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigKVHandler))
			adminRouter.Methods(http.MethodDelete).Path(adminVersion + "/del-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.DelConfigKVHandler))
		}

		// Enable config help in all modes.
		adminRouter.Methods(http.MethodGet).Path(adminVersion+"/help-config-kv").HandlerFunc(httpTraceAll(adminAPI.HelpConfigKVHandler)).Queries("subSys", "{subSys:.*}", "key", "{key:.*}")

		// Config KV history operations.
		if enableConfigOps {
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-config-history-kv").HandlerFunc(httpTraceAll(adminAPI.ListConfigHistoryKVHandler)).Queries("count", "{count:[0-9]+}")
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/clear-config-history-kv").HandlerFunc(httpTraceHdrs(adminAPI.ClearConfigHistoryKVHandler)).Queries("restoreId", "{restoreId:.*}")
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/restore-config-history-kv").HandlerFunc(httpTraceHdrs(adminAPI.RestoreConfigHistoryKVHandler)).Queries("restoreId", "{restoreId:.*}")
		}

		/// Config import/export bulk operations
		if enableConfigOps {
			// Get config
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/config").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigHandler))
			// Set config
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/config").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigHandler))
		}

		if enableIAMOps {
			// -- IAM APIs --

			// Add policy IAM
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/add-canned-policy").HandlerFunc(httpTraceAll(adminAPI.AddCannedPolicy)).Queries("name", "{name:.*}")

			// Add user IAM
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/accountinfo").HandlerFunc(httpTraceAll(adminAPI.AccountInfoHandler))

			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/add-user").HandlerFunc(httpTraceHdrs(adminAPI.AddUser)).Queries("accessKey", "{accessKey:.*}")

			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-user-status").HandlerFunc(httpTraceHdrs(adminAPI.SetUserStatus)).Queries("accessKey", "{accessKey:.*}").Queries("status", "{status:.*}")

			// Service accounts ops
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/add-service-account").HandlerFunc(httpTraceHdrs(adminAPI.AddServiceAccount))
			adminRouter.Methods(http.MethodPost).Path(adminVersion+"/update-service-account").HandlerFunc(httpTraceHdrs(adminAPI.UpdateServiceAccount)).Queries("accessKey", "{accessKey:.*}")
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-service-account").HandlerFunc(httpTraceHdrs(adminAPI.InfoServiceAccount)).Queries("accessKey", "{accessKey:.*}")
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-service-accounts").HandlerFunc(httpTraceHdrs(adminAPI.ListServiceAccounts))
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/delete-service-account").HandlerFunc(httpTraceHdrs(adminAPI.DeleteServiceAccount)).Queries("accessKey", "{accessKey:.*}")

			// Info policy IAM latest
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.InfoCannedPolicy)).Queries("name", "{name:.*}")
			// List policies latest
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-canned-policies").HandlerFunc(httpTraceHdrs(adminAPI.ListBucketPolicies)).Queries("bucket", "{bucket:.*}")
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-canned-policies").HandlerFunc(httpTraceHdrs(adminAPI.ListCannedPolicies))

			// Remove policy IAM
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.RemoveCannedPolicy)).Queries("name", "{name:.*}")

			// Set user or group policy
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-user-or-group-policy").
				HandlerFunc(httpTraceHdrs(adminAPI.SetPolicyForUserOrGroup)).
				Queries("policyName", "{policyName:.*}", "userOrGroup", "{userOrGroup:.*}", "isGroup", "{isGroup:true|false}")

			// Remove user IAM
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-user").HandlerFunc(httpTraceHdrs(adminAPI.RemoveUser)).Queries("accessKey", "{accessKey:.*}")

			// List users
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-users").HandlerFunc(httpTraceHdrs(adminAPI.ListBucketUsers)).Queries("bucket", "{bucket:.*}")
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-users").HandlerFunc(httpTraceHdrs(adminAPI.ListUsers))

			// User info
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/user-info").HandlerFunc(httpTraceHdrs(adminAPI.GetUserInfo)).Queries("accessKey", "{accessKey:.*}")
			// Add/Remove members from group
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/update-group-members").HandlerFunc(httpTraceHdrs(adminAPI.UpdateGroupMembers))

			// Get Group
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/group").HandlerFunc(httpTraceHdrs(adminAPI.GetGroup)).Queries("group", "{group:.*}")

			// List Groups
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/groups").HandlerFunc(httpTraceHdrs(adminAPI.ListGroups))

			// Set Group Status
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-group-status").HandlerFunc(httpTraceHdrs(adminAPI.SetGroupStatus)).Queries("group", "{group:.*}").Queries("status", "{status:.*}")
		}

		if globalIsDistErasure || globalIsErasure {
			// GetBucketQuotaConfig
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/get-bucket-quota").HandlerFunc(
				httpTraceHdrs(adminAPI.GetBucketQuotaConfigHandler)).Queries("bucket", "{bucket:.*}")
			// PutBucketQuotaConfig
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-bucket-quota").HandlerFunc(
				httpTraceHdrs(adminAPI.PutBucketQuotaConfigHandler)).Queries("bucket", "{bucket:.*}")

			// Bucket replication operations
			// GetBucketTargetHandler
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/list-remote-targets").HandlerFunc(
				httpTraceHdrs(adminAPI.ListRemoteTargetsHandler)).Queries("bucket", "{bucket:.*}", "type", "{type:.*}")
			// SetRemoteTargetHandler
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-remote-target").HandlerFunc(
				httpTraceHdrs(adminAPI.SetRemoteTargetHandler)).Queries("bucket", "{bucket:.*}")
			// RemoveRemoteTargetHandler
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-remote-target").HandlerFunc(
				httpTraceHdrs(adminAPI.RemoveRemoteTargetHandler)).Queries("bucket", "{bucket:.*}", "arn", "{arn:.*}")

			// Remote Tier management operations
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/tier").HandlerFunc(httpTraceHdrs(adminAPI.AddTierHandler))
			adminRouter.Methods(http.MethodPost).Path(adminVersion + "/tier/{tier}").HandlerFunc(httpTraceHdrs(adminAPI.EditTierHandler))
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/tier").HandlerFunc(httpTraceHdrs(adminAPI.ListTierHandler))
		}

		if globalIsDistErasure {
			// Top locks
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/top/locks").HandlerFunc(httpTraceHdrs(adminAPI.TopLocksHandler))
			// Force unlocks paths
			adminRouter.Methods(http.MethodPost).Path(adminVersion+"/force-unlock").
				Queries("paths", "{paths:.*}").HandlerFunc(httpTraceHdrs(adminAPI.ForceUnlockHandler))
		}

		// HTTP Trace
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/trace").HandlerFunc(adminAPI.TraceHandler)

		// Console Logs
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/log").HandlerFunc(httpTraceAll(adminAPI.ConsoleLogHandler))

		// -- KMS APIs --
		//
		adminRouter.Methods(http.MethodPost).Path(adminVersion + "/kms/status").HandlerFunc(httpTraceAll(adminAPI.KMSStatusHandler))
		adminRouter.Methods(http.MethodPost).Path(adminVersion+"/kms/key/create").HandlerFunc(httpTraceAll(adminAPI.KMSCreateKeyHandler)).Queries("key-id", "{key-id:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/kms/key/status").HandlerFunc(httpTraceAll(adminAPI.KMSKeyStatusHandler))

		if !globalIsGateway {
			// Keep obdinfo for backward compatibility with mc
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/obdinfo").
				HandlerFunc(httpTraceHdrs(adminAPI.HealthInfoHandler))
			// -- Health API --
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/healthinfo").
				HandlerFunc(httpTraceHdrs(adminAPI.HealthInfoHandler))
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/bandwidth").
				HandlerFunc(httpTraceHdrs(adminAPI.BandwidthMonitorHandler))
		}
	}

	// If none of the routes match add default error handler routes
	adminRouter.NotFoundHandler = httpTraceAll(errorResponseHandler)
	adminRouter.MethodNotAllowedHandler = httpTraceAll(methodNotAllowedHandler("Admin"))
}
