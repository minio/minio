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
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/madmin"
)

const (
	adminPathPrefix         = minioReservedBucketPath + "/admin"
	adminAPIVersionV2       = madmin.AdminAPIVersionV2
	adminAPIVersion         = madmin.AdminAPIVersion
	adminAPIVersionPrefix   = SlashSeparator + adminAPIVersion
	adminAPIVersionV2Prefix = SlashSeparator + adminAPIVersionV2
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
		adminAPIVersionV2Prefix,
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
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/add-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.AddCannedPolicy)).Queries("name", "{name:.*}")

			// Add user IAM

			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/accountusageinfo").HandlerFunc(httpTraceAll(adminAPI.AccountUsageInfoHandler))

			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/add-user").HandlerFunc(httpTraceHdrs(adminAPI.AddUser)).Queries("accessKey", "{accessKey:.*}")

			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-user-status").HandlerFunc(httpTraceHdrs(adminAPI.SetUserStatus)).Queries("accessKey", "{accessKey:.*}").Queries("status", "{status:.*}")

			// Service accounts ops
			adminRouter.Methods(http.MethodPut).Path(adminVersion + "/add-service-account").HandlerFunc(httpTraceHdrs(adminAPI.AddServiceAccount))
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-service-accounts").HandlerFunc(httpTraceHdrs(adminAPI.ListServiceAccounts))
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/delete-service-account").HandlerFunc(httpTraceHdrs(adminAPI.DeleteServiceAccount)).Queries("accessKey", "{accessKey:.*}")

			if adminVersion == adminAPIVersionV2Prefix {
				// Info policy IAM v2
				adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.InfoCannedPolicyV2)).Queries("name", "{name:.*}")

				// List policies v2
				adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-canned-policies").HandlerFunc(httpTraceHdrs(adminAPI.ListCannedPoliciesV2))
			} else {
				// Info policy IAM latest
				adminRouter.Methods(http.MethodGet).Path(adminVersion+"/info-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.InfoCannedPolicy)).Queries("name", "{name:.*}")

				// List policies latest
				adminRouter.Methods(http.MethodGet).Path(adminVersion + "/list-canned-policies").HandlerFunc(httpTraceHdrs(adminAPI.ListCannedPolicies))
			}

			// Remove policy IAM
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.RemoveCannedPolicy)).Queries("name", "{name:.*}")

			// Set user or group policy
			adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-user-or-group-policy").
				HandlerFunc(httpTraceHdrs(adminAPI.SetPolicyForUserOrGroup)).
				Queries("policyName", "{policyName:.*}", "userOrGroup", "{userOrGroup:.*}", "isGroup", "{isGroup:true|false}")

			// Remove user IAM
			adminRouter.Methods(http.MethodDelete).Path(adminVersion+"/remove-user").HandlerFunc(httpTraceHdrs(adminAPI.RemoveUser)).Queries("accessKey", "{accessKey:.*}")

			// List users
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

		// Quota operations
		if globalIsDistErasure || globalIsErasure {
			if env.Get(envDataUsageCrawlConf, config.EnableOn) == config.EnableOn {
				// GetBucketQuotaConfig
				adminRouter.Methods(http.MethodGet).Path(adminVersion+"/get-bucket-quota").HandlerFunc(
					httpTraceHdrs(adminAPI.GetBucketQuotaConfigHandler)).Queries("bucket", "{bucket:.*}")
				// PutBucketQuotaConfig
				adminRouter.Methods(http.MethodPut).Path(adminVersion+"/set-bucket-quota").HandlerFunc(
					httpTraceHdrs(adminAPI.PutBucketQuotaConfigHandler)).Queries("bucket", "{bucket:.*}")
			}
		}

		// -- Top APIs --
		// Top locks
		if globalIsDistErasure {
			adminRouter.Methods(http.MethodGet).Path(adminVersion + "/top/locks").HandlerFunc(httpTraceHdrs(adminAPI.TopLocksHandler))
		}

		// HTTP Trace
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/trace").HandlerFunc(adminAPI.TraceHandler)

		// Console Logs
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/log").HandlerFunc(httpTraceAll(adminAPI.ConsoleLogHandler))

		// -- KMS APIs --
		//
		adminRouter.Methods(http.MethodGet).Path(adminVersion + "/kms/key/status").HandlerFunc(httpTraceAll(adminAPI.KMSKeyStatusHandler))

		if !globalIsGateway {
			// -- OBD API --
			adminRouter.Methods(http.MethodGet).Path(adminVersion+"/obdinfo").
				HandlerFunc(httpTraceHdrs(adminAPI.OBDInfoHandler)).
				Queries("perfdrive", "{perfdrive:true|false}",
					"perfnet", "{perfnet:true|false}",
					"minioinfo", "{minioinfo:true|false}",
					"minioconfig", "{minioconfig:true|false}",
					"syscpu", "{syscpu:true|false}",
					"sysdiskhw", "{sysdiskhw:true|false}",
					"sysosinfo", "{sysosinfo:true|false}",
					"sysmem", "{sysmem:true|false}",
					"sysprocess", "{sysprocess:true|false}",
				)
		}
	}

	// If none of the routes match add default error handler routes
	adminRouter.NotFoundHandler = http.HandlerFunc(httpTraceAll(errorResponseHandler))
	adminRouter.MethodNotAllowedHandler = http.HandlerFunc(httpTraceAll(errorResponseHandler))
}
