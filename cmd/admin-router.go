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
	adminPathPrefix       = minioReservedBucketPath + "/admin"
	adminAPIVersion       = madmin.AdminAPIVersion
	adminAPIVersionPrefix = SlashSeparator + madmin.AdminAPIVersion
)

// adminAPIHandlers provides HTTP handlers for MinIO admin API.
type adminAPIHandlers struct{}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(router *mux.Router, enableConfigOps, enableIAMOps bool) {

	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := router.PathPrefix(adminPathPrefix).Subrouter()

	/// Service operations

	// Restart and stop MinIO service.
	adminRouter.Methods(http.MethodPost).Path(adminAPIVersionPrefix+"/service").HandlerFunc(httpTraceAll(adminAPI.ServiceActionHandler)).Queries("action", "{action:.*}")
	// Update MinIO servers.
	adminRouter.Methods(http.MethodPost).Path(adminAPIVersionPrefix+"/update").HandlerFunc(httpTraceAll(adminAPI.ServerUpdateHandler)).Queries("updateURL", "{updateURL:.*}")

	// Info operations
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/info").HandlerFunc(httpTraceAll(adminAPI.ServerInfoHandler))
	// Harware Info operations
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix+"/hardware").HandlerFunc(httpTraceAll(adminAPI.ServerHardwareInfoHandler)).Queries("hwType", "{hwType:.*}")

	// StorageInfo operations
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/storageinfo").HandlerFunc(httpTraceAll(adminAPI.StorageInfoHandler))
	// DataUsageInfo operations
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/datausageinfo").HandlerFunc(httpTraceAll(adminAPI.DataUsageInfoHandler))

	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/accountingusageinfo").HandlerFunc(httpTraceAll(adminAPI.AccountingUsageInfoHandler))

	if globalIsDistXL || globalIsXL {
		/// Heal operations

		// Heal processing endpoint.
		adminRouter.Methods(http.MethodPost).Path(adminAPIVersionPrefix + "/heal/").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
		adminRouter.Methods(http.MethodPost).Path(adminAPIVersionPrefix + "/heal/{bucket}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
		adminRouter.Methods(http.MethodPost).Path(adminAPIVersionPrefix + "/heal/{bucket}/{prefix:.*}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))

		adminRouter.Methods(http.MethodPost).Path(adminAPIVersionPrefix + "/background-heal/status").HandlerFunc(httpTraceAll(adminAPI.BackgroundHealStatusHandler))

		/// Health operations

	}
	// Performance command - return performance details based on input type
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix+"/performance").HandlerFunc(httpTraceAll(adminAPI.PerfInfoHandler)).Queries("perfType", "{perfType:.*}")

	// Profiling operations
	adminRouter.Methods(http.MethodPost).Path(adminAPIVersionPrefix+"/profiling/start").HandlerFunc(httpTraceAll(adminAPI.StartProfilingHandler)).
		Queries("profilerType", "{profilerType:.*}")
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/profiling/download").HandlerFunc(httpTraceAll(adminAPI.DownloadProfilingHandler))

	// Config KV operations.
	if enableConfigOps {
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix+"/get-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigKVHandler)).Queries("key", "{key:.*}")
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix + "/set-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigKVHandler))
		adminRouter.Methods(http.MethodDelete).Path(adminAPIVersionPrefix + "/del-config-kv").HandlerFunc(httpTraceHdrs(adminAPI.DelConfigKVHandler))
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix+"/help-config-kv").HandlerFunc(httpTraceAll(adminAPI.HelpConfigKVHandler)).Queries("subSys", "{subSys:.*}", "key", "{key:.*}")
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix+"/list-config-history-kv").HandlerFunc(httpTraceAll(adminAPI.ListConfigHistoryKVHandler)).Queries("count", "{count:[0-9]+}")
		adminRouter.Methods(http.MethodDelete).Path(adminAPIVersionPrefix+"/clear-config-history-kv").HandlerFunc(httpTraceHdrs(adminAPI.ClearConfigHistoryKVHandler)).Queries("restoreId", "{restoreId:.*}")
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix+"/restore-config-history-kv").HandlerFunc(httpTraceHdrs(adminAPI.RestoreConfigHistoryKVHandler)).Queries("restoreId", "{restoreId:.*}")
	}

	/// Config operations
	if enableConfigOps {
		// Get config
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/config").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigHandler))
		// Set config
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix + "/config").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigHandler))
	}

	if enableIAMOps {
		// -- IAM APIs --

		// Add policy IAM
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix+"/add-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.AddCannedPolicy)).Queries("name",
			"{name:.*}")

		// Add user IAM
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix+"/add-user").HandlerFunc(httpTraceHdrs(adminAPI.AddUser)).Queries("accessKey", "{accessKey:.*}")
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix+"/set-user-status").HandlerFunc(httpTraceHdrs(adminAPI.SetUserStatus)).
			Queries("accessKey", "{accessKey:.*}").Queries("status", "{status:.*}")

		// Info policy IAM
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix+"/info-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.InfoCannedPolicy)).Queries("name", "{name:.*}")

		// Remove policy IAM
		adminRouter.Methods(http.MethodDelete).Path(adminAPIVersionPrefix+"/remove-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.RemoveCannedPolicy)).Queries("name", "{name:.*}")

		// Set user or group policy
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix+"/set-user-or-group-policy").
			HandlerFunc(httpTraceHdrs(adminAPI.SetPolicyForUserOrGroup)).
			Queries("policyName", "{policyName:.*}", "userOrGroup", "{userOrGroup:.*}", "isGroup", "{isGroup:true|false}")

		// Remove user IAM
		adminRouter.Methods(http.MethodDelete).Path(adminAPIVersionPrefix+"/remove-user").HandlerFunc(httpTraceHdrs(adminAPI.RemoveUser)).Queries("accessKey", "{accessKey:.*}")

		// List users
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/list-users").HandlerFunc(httpTraceHdrs(adminAPI.ListUsers))

		// User info
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix+"/user-info").HandlerFunc(httpTraceHdrs(adminAPI.GetUserInfo)).Queries("accessKey", "{accessKey:.*}")

		// Add/Remove members from group
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix + "/update-group-members").HandlerFunc(httpTraceHdrs(adminAPI.UpdateGroupMembers))

		// Get Group
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix+"/group").HandlerFunc(httpTraceHdrs(adminAPI.GetGroup)).Queries("group", "{group:.*}")

		// List Groups
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/groups").HandlerFunc(httpTraceHdrs(adminAPI.ListGroups))

		// Set Group Status
		adminRouter.Methods(http.MethodPut).Path(adminAPIVersionPrefix+"/set-group-status").HandlerFunc(httpTraceHdrs(adminAPI.SetGroupStatus)).Queries("group", "{group:.*}").Queries("status", "{status:.*}")

		// List policies
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/list-canned-policies").HandlerFunc(httpTraceHdrs(adminAPI.ListCannedPolicies))
	}

	// -- Top APIs --
	// Top locks
	if globalIsDistXL {
		adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/top/locks").HandlerFunc(httpTraceHdrs(adminAPI.TopLocksHandler))
	}

	// HTTP Trace
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/trace").HandlerFunc(adminAPI.TraceHandler)

	// Console Logs
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/log").HandlerFunc(httpTraceAll(adminAPI.ConsoleLogHandler))

	// -- KMS APIs --
	//
	adminRouter.Methods(http.MethodGet).Path(adminAPIVersionPrefix + "/kms/key/status").HandlerFunc(httpTraceAll(adminAPI.KMSKeyStatusHandler))

	// If none of the routes match add default error handler routes
	adminRouter.NotFoundHandler = http.HandlerFunc(httpTraceAll(errorResponseHandler))
	adminRouter.MethodNotAllowedHandler = http.HandlerFunc(httpTraceAll(errorResponseHandler))
}
