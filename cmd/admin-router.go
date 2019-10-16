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
)

const (
	adminAPIPathPrefix = "/minio/admin"
)

// adminAPIHandlers provides HTTP handlers for MinIO admin API.
type adminAPIHandlers struct {
}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(router *mux.Router, enableConfigOps, enableIAMOps bool) {

	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := router.PathPrefix(adminAPIPathPrefix).Subrouter()

	// Version handler
	adminV1Router := adminRouter.PathPrefix("/v1").Subrouter()

	/// Service operations

	// Restart and stop MinIO service.
	adminV1Router.Methods(http.MethodPost).Path("/service").HandlerFunc(httpTraceAll(adminAPI.ServiceActionHandler)).Queries("action", "{action:.*}")
	// Update MinIO servers.
	adminV1Router.Methods(http.MethodPost).Path("/update").HandlerFunc(httpTraceAll(adminAPI.ServerUpdateHandler)).Queries("updateURL", "{updateURL:.*}")

	// Info operations
	adminV1Router.Methods(http.MethodGet).Path("/info").HandlerFunc(httpTraceAll(adminAPI.ServerInfoHandler))
	// Harware Info operations
	adminV1Router.Methods(http.MethodGet).Path("/hardware").HandlerFunc(httpTraceAll(adminAPI.ServerHardwareInfoHandler)).Queries("hwType", "{hwType:.*}")

	if globalIsDistXL || globalIsXL {
		/// Heal operations

		// Heal processing endpoint.
		adminV1Router.Methods(http.MethodPost).Path("/heal/").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
		adminV1Router.Methods(http.MethodPost).Path("/heal/{bucket}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
		adminV1Router.Methods(http.MethodPost).Path("/heal/{bucket}/{prefix:.*}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))

		adminV1Router.Methods(http.MethodPost).Path("/background-heal/status").HandlerFunc(httpTraceAll(adminAPI.BackgroundHealStatusHandler))

		/// Health operations

	}
	// Performance command - return performance details based on input type
	adminV1Router.Methods(http.MethodGet).Path("/performance").HandlerFunc(httpTraceAll(adminAPI.PerfInfoHandler)).Queries("perfType", "{perfType:.*}")

	// Profiling operations
	adminV1Router.Methods(http.MethodPost).Path("/profiling/start").HandlerFunc(httpTraceAll(adminAPI.StartProfilingHandler)).
		Queries("profilerType", "{profilerType:.*}")
	adminV1Router.Methods(http.MethodGet).Path("/profiling/download").HandlerFunc(httpTraceAll(adminAPI.DownloadProfilingHandler))

	/// Config operations
	if enableConfigOps {
		// Get config
		adminV1Router.Methods(http.MethodGet).Path("/config").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigHandler))
		// Set config
		adminV1Router.Methods(http.MethodPut).Path("/config").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigHandler))
	}

	if enableIAMOps {
		// -- IAM APIs --

		// Add policy IAM
		adminV1Router.Methods(http.MethodPut).Path("/add-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.AddCannedPolicy)).Queries("name",
			"{name:.*}")

		// Add user IAM
		adminV1Router.Methods(http.MethodPut).Path("/add-user").HandlerFunc(httpTraceHdrs(adminAPI.AddUser)).Queries("accessKey", "{accessKey:.*}")
		adminV1Router.Methods(http.MethodPut).Path("/set-user-status").HandlerFunc(httpTraceHdrs(adminAPI.SetUserStatus)).
			Queries("accessKey", "{accessKey:.*}").Queries("status", "{status:.*}")

		// Info policy IAM
		adminV1Router.Methods(http.MethodGet).Path("/info-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.InfoCannedPolicy)).Queries("name", "{name:.*}")

		// Remove policy IAM
		adminV1Router.Methods(http.MethodDelete).Path("/remove-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.RemoveCannedPolicy)).Queries("name", "{name:.*}")

		// Set user or group policy
		adminV1Router.Methods(http.MethodPut).Path("/set-user-or-group-policy").
			HandlerFunc(httpTraceHdrs(adminAPI.SetPolicyForUserOrGroup)).
			Queries("policyName", "{policyName:.*}", "userOrGroup", "{userOrGroup:.*}", "isGroup", "{isGroup:true|false}")

		// Remove user IAM
		adminV1Router.Methods(http.MethodDelete).Path("/remove-user").HandlerFunc(httpTraceHdrs(adminAPI.RemoveUser)).Queries("accessKey", "{accessKey:.*}")

		// List users
		adminV1Router.Methods(http.MethodGet).Path("/list-users").HandlerFunc(httpTraceHdrs(adminAPI.ListUsers))

		// User info
		adminV1Router.Methods(http.MethodGet).Path("/user-info").HandlerFunc(httpTraceHdrs(adminAPI.GetUserInfo)).Queries("accessKey", "{accessKey:.*}")

		// Add/Remove members from group
		adminV1Router.Methods(http.MethodPut).Path("/update-group-members").HandlerFunc(httpTraceHdrs(adminAPI.UpdateGroupMembers))

		// Get Group
		adminV1Router.Methods(http.MethodGet).Path("/group").HandlerFunc(httpTraceHdrs(adminAPI.GetGroup)).Queries("group", "{group:.*}")

		// List Groups
		adminV1Router.Methods(http.MethodGet).Path("/groups").HandlerFunc(httpTraceHdrs(adminAPI.ListGroups))

		// Set Group Status
		adminV1Router.Methods(http.MethodPut).Path("/set-group-status").HandlerFunc(httpTraceHdrs(adminAPI.SetGroupStatus)).Queries("group", "{group:.*}").Queries("status", "{status:.*}")

		// List policies
		adminV1Router.Methods(http.MethodGet).Path("/list-canned-policies").HandlerFunc(httpTraceHdrs(adminAPI.ListCannedPolicies))
	}

	// -- Top APIs --
	// Top locks
	adminV1Router.Methods(http.MethodGet).Path("/top/locks").HandlerFunc(httpTraceHdrs(adminAPI.TopLocksHandler))

	// HTTP Trace
	adminV1Router.Methods(http.MethodGet).Path("/trace").HandlerFunc(adminAPI.TraceHandler)

	// Console Logs
	adminV1Router.Methods(http.MethodGet).Path("/log").HandlerFunc(httpTraceAll(adminAPI.ConsoleLogHandler))

	// -- KMS APIs --
	//
	adminV1Router.Methods(http.MethodGet).Path("/kms/key/status").HandlerFunc(httpTraceAll(adminAPI.KMSKeyStatusHandler))

	// If none of the routes match, return error.
	adminV1Router.NotFoundHandler = http.HandlerFunc(httpTraceHdrs(notFoundHandler))
	adminV1Router.MethodNotAllowedHandler = http.HandlerFunc(httpTraceAll(versionMismatchHandler))
}
