/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018, 2019 MinIO, Inc.
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

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/madmin"
)

const (
	adminAPIPathPrefix = minioReservedBucketPath + "/admin/" + madmin.AdminAPIVersion
)

// adminAPIHandlers provides HTTP handlers for MinIO admin API.
type adminAPIHandlers struct {
}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(router *mux.Router, enableConfigOps, enableIAMOps bool) {

	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := router.PathPrefix(adminAPIPathPrefix).Subrouter()

	/// Service operations

	// Restart and stop MinIO service.
	adminRouter.Methods(http.MethodPost).Path("/service").HandlerFunc(httpTraceAll(adminAPI.ServiceActionHandler)).Queries("action", "{action:.*}")
	// Update MinIO servers.
	adminRouter.Methods(http.MethodPost).Path("/update").HandlerFunc(httpTraceAll(adminAPI.ServerUpdateHandler)).Queries("updateURL", "{updateURL:.*}")

	// Info operations
	adminRouter.Methods(http.MethodGet).Path("/info").HandlerFunc(httpTraceAll(adminAPI.ServerInfoHandler))
	// Harware Info operations
	adminRouter.Methods(http.MethodGet).Path("/hardware").HandlerFunc(httpTraceAll(adminAPI.ServerHardwareInfoHandler)).Queries("hwType", "{hwType:.*}")

	// StorageInfo operations
	adminRouter.Methods(http.MethodGet).Path("/storageinfo").HandlerFunc(httpTraceAll(adminAPI.StorageInfoHandler))

	if globalIsDistXL || globalIsXL {
		/// Heal operations

		// Heal processing endpoint.
		adminRouter.Methods(http.MethodPost).Path("/heal/").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
		adminRouter.Methods(http.MethodPost).Path("/heal/{bucket}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
		adminRouter.Methods(http.MethodPost).Path("/heal/{bucket}/{prefix:.*}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))

		adminRouter.Methods(http.MethodPost).Path("/background-heal/status").HandlerFunc(httpTraceAll(adminAPI.BackgroundHealStatusHandler))

		/// Health operations

	}
	// Performance command - return performance details based on input type
	adminRouter.Methods(http.MethodGet).Path("/performance").HandlerFunc(httpTraceAll(adminAPI.PerfInfoHandler)).Queries("perfType", "{perfType:.*}")

	// Profiling operations
	adminRouter.Methods(http.MethodPost).Path("/profiling/start").HandlerFunc(httpTraceAll(adminAPI.StartProfilingHandler)).
		Queries("profilerType", "{profilerType:.*}")
	adminRouter.Methods(http.MethodGet).Path("/profiling/download").HandlerFunc(httpTraceAll(adminAPI.DownloadProfilingHandler))

	// Config KV operations.
	if enableConfigOps {
		adminRouter.Methods(http.MethodGet).Path("/get-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigKVHandler)).Queries("key", "{key:.*}")
		adminRouter.Methods(http.MethodPut).Path("/set-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigKVHandler))
		adminRouter.Methods(http.MethodDelete).Path("/del-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.DelConfigKVHandler))
		adminRouter.Methods(http.MethodGet).Path("/help-config-kv").HandlerFunc(httpTraceAll(adminAPI.HelpConfigKVHandler)).Queries("subSys", "{subSys:.*}", "key", "{key:.*}")
		adminRouter.Methods(http.MethodGet).Path("/list-config-history-kv").HandlerFunc(httpTraceAll(adminAPI.ListConfigHistoryKVHandler))
		adminRouter.Methods(http.MethodDelete).Path("/clear-config-history-kv").HandlerFunc(httpTraceHdrs(adminAPI.ClearConfigHistoryKVHandler)).Queries("restoreId", "{restoreId:.*}")
		adminRouter.Methods(http.MethodPut).Path("/restore-config-history-kv").HandlerFunc(httpTraceHdrs(adminAPI.RestoreConfigHistoryKVHandler)).Queries("restoreId", "{restoreId:.*}")
	}

	/// Config operations
	if enableConfigOps {
		// Get config
		adminRouter.Methods(http.MethodGet).Path("/config").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigHandler))
		// Set config
		adminRouter.Methods(http.MethodPut).Path("/config").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigHandler))
	}

	if enableIAMOps {
		// -- IAM APIs --

		// Add policy IAM
		adminRouter.Methods(http.MethodPut).Path("/add-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.AddCannedPolicy)).Queries("name",
			"{name:.*}")

		// Add user IAM
		adminRouter.Methods(http.MethodPut).Path("/add-user").HandlerFunc(httpTraceHdrs(adminAPI.AddUser)).Queries("accessKey", "{accessKey:.*}")
		adminRouter.Methods(http.MethodPut).Path("/set-user-status").HandlerFunc(httpTraceHdrs(adminAPI.SetUserStatus)).
			Queries("accessKey", "{accessKey:.*}").Queries("status", "{status:.*}")

		// Info policy IAM
		adminRouter.Methods(http.MethodGet).Path("/info-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.InfoCannedPolicy)).Queries("name", "{name:.*}")

		// Remove policy IAM
		adminRouter.Methods(http.MethodDelete).Path("/remove-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.RemoveCannedPolicy)).Queries("name", "{name:.*}")

		// Set user or group policy
		adminRouter.Methods(http.MethodPut).Path("/set-user-or-group-policy").
			HandlerFunc(httpTraceHdrs(adminAPI.SetPolicyForUserOrGroup)).
			Queries("policyName", "{policyName:.*}", "userOrGroup", "{userOrGroup:.*}", "isGroup", "{isGroup:true|false}")

		// Remove user IAM
		adminRouter.Methods(http.MethodDelete).Path("/remove-user").HandlerFunc(httpTraceHdrs(adminAPI.RemoveUser)).Queries("accessKey", "{accessKey:.*}")

		// List users
		adminRouter.Methods(http.MethodGet).Path("/list-users").HandlerFunc(httpTraceHdrs(adminAPI.ListUsers))

		// User info
		adminRouter.Methods(http.MethodGet).Path("/user-info").HandlerFunc(httpTraceHdrs(adminAPI.GetUserInfo)).Queries("accessKey", "{accessKey:.*}")

		// Add/Remove members from group
		adminRouter.Methods(http.MethodPut).Path("/update-group-members").HandlerFunc(httpTraceHdrs(adminAPI.UpdateGroupMembers))

		// Get Group
		adminRouter.Methods(http.MethodGet).Path("/group").HandlerFunc(httpTraceHdrs(adminAPI.GetGroup)).Queries("group", "{group:.*}")

		// List Groups
		adminRouter.Methods(http.MethodGet).Path("/groups").HandlerFunc(httpTraceHdrs(adminAPI.ListGroups))

		// Set Group Status
		adminRouter.Methods(http.MethodPut).Path("/set-group-status").HandlerFunc(httpTraceHdrs(adminAPI.SetGroupStatus)).Queries("group", "{group:.*}").Queries("status", "{status:.*}")

		// List policies
		adminRouter.Methods(http.MethodGet).Path("/list-canned-policies").HandlerFunc(httpTraceHdrs(adminAPI.ListCannedPolicies))
	}

	// -- Top APIs --
	// Top locks
	adminRouter.Methods(http.MethodGet).Path("/top/locks").HandlerFunc(httpTraceHdrs(adminAPI.TopLocksHandler))

	// HTTP Trace
	adminRouter.Methods(http.MethodGet).Path("/trace").HandlerFunc(adminAPI.TraceHandler)

	// Console Logs
	adminRouter.Methods(http.MethodGet).Path("/log").HandlerFunc(httpTraceAll(adminAPI.ConsoleLogHandler))

	// -- KMS APIs --
	//
	adminRouter.Methods(http.MethodGet).Path("/kms/key/status").HandlerFunc(httpTraceAll(adminAPI.KMSKeyStatusHandler))

	// If none of the routes match, return error.
	adminRouter.NotFoundHandler = http.HandlerFunc(httpTraceHdrs(notFoundHandler))
	adminRouter.MethodNotAllowedHandler = http.HandlerFunc(httpTraceAll(versionMismatchHandler))
}
