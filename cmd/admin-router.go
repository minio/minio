/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

// adminAPIHandlers provides HTTP handlers for Minio admin API.
type adminAPIHandlers struct {
}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(router *mux.Router, enableIAM bool) {

	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := router.PathPrefix(adminAPIPathPrefix).Subrouter()

	// Version handler
	adminRouter.Methods(http.MethodGet).Path("/version").HandlerFunc(httpTraceAll(adminAPI.VersionHandler))

	adminV1Router := adminRouter.PathPrefix("/v1").Subrouter()

	/// Service operations

	// Service status
	adminV1Router.Methods(http.MethodGet).Path("/service").HandlerFunc(httpTraceAll(adminAPI.ServiceStatusHandler))

	// Service restart and stop - TODO
	adminV1Router.Methods(http.MethodPost).Path("/service").HandlerFunc(httpTraceAll(adminAPI.ServiceStopNRestartHandler))

	// Info operations
	adminV1Router.Methods(http.MethodGet).Path("/info").HandlerFunc(httpTraceAll(adminAPI.ServerInfoHandler))

	if globalIsDistXL || globalIsXL {
		/// Heal operations

		// Heal processing endpoint.
		adminV1Router.Methods(http.MethodPost).Path("/heal/").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
		adminV1Router.Methods(http.MethodPost).Path("/heal/{bucket}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
		adminV1Router.Methods(http.MethodPost).Path("/heal/{bucket}/{prefix:.*}").HandlerFunc(httpTraceAll(adminAPI.HealHandler))
	}

	// Profiling operations
	adminV1Router.Methods(http.MethodPost).Path("/profiling/start").HandlerFunc(httpTraceAll(adminAPI.StartProfilingHandler)).
		Queries("profilerType", "{profilerType:.*}")
	adminV1Router.Methods(http.MethodGet).Path("/profiling/download").HandlerFunc(httpTraceAll(adminAPI.DownloadProfilingHandler))

	/// Config operations

	if enableIAM {
		// Update credentials
		adminV1Router.Methods(http.MethodPut).Path("/config/credential").HandlerFunc(httpTraceHdrs(adminAPI.UpdateAdminCredentialsHandler))
		// Get config
		adminV1Router.Methods(http.MethodGet).Path("/config").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigHandler))
		// Set config
		adminV1Router.Methods(http.MethodPut).Path("/config").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigHandler))

		// Get config keys/values
		adminV1Router.Methods(http.MethodGet).Path("/config-keys").HandlerFunc(httpTraceHdrs(adminAPI.GetConfigKeysHandler))
		// Set config keys/values
		adminV1Router.Methods(http.MethodPut).Path("/config-keys").HandlerFunc(httpTraceHdrs(adminAPI.SetConfigKeysHandler))

		// -- IAM APIs --

		// Add policy IAM
		adminV1Router.Methods(http.MethodPut).Path("/add-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.AddCannedPolicy)).Queries("name", "{name:.*}")

		// Add user IAM
		adminV1Router.Methods(http.MethodPut).Path("/add-user").HandlerFunc(httpTraceHdrs(adminAPI.AddUser)).Queries("accessKey", "{accessKey:.*}")
		adminV1Router.Methods(http.MethodPut).Path("/set-user-policy").HandlerFunc(httpTraceHdrs(adminAPI.SetUserPolicy)).
			Queries("accessKey", "{accessKey:.*}").Queries("name", "{name:.*}")
		adminV1Router.Methods(http.MethodPut).Path("/set-user-status").HandlerFunc(httpTraceHdrs(adminAPI.SetUserStatus)).
			Queries("accessKey", "{accessKey:.*}").Queries("status", "{status:.*}")

		// Remove policy IAM
		adminV1Router.Methods(http.MethodDelete).Path("/remove-canned-policy").HandlerFunc(httpTraceHdrs(adminAPI.RemoveCannedPolicy)).Queries("name", "{name:.*}")

		// Remove user IAM
		adminV1Router.Methods(http.MethodDelete).Path("/remove-user").HandlerFunc(httpTraceHdrs(adminAPI.RemoveUser)).Queries("accessKey", "{accessKey:.*}")

		// List users
		adminV1Router.Methods(http.MethodGet).Path("/list-users").HandlerFunc(httpTraceHdrs(adminAPI.ListUsers))

		// List policies
		adminV1Router.Methods(http.MethodGet).Path("/list-canned-policies").HandlerFunc(httpTraceHdrs(adminAPI.ListCannedPolicies))
	}

	// If none of the routes match, return error.
	adminV1Router.NotFoundHandler = http.HandlerFunc(httpTraceHdrs(notFoundHandlerJSON))
}
