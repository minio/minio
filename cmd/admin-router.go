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

	router "github.com/gorilla/mux"
)

const (
	adminAPIPathPrefix = "/minio/admin"
)

// adminAPIHandlers provides HTTP handlers for Minio admin API.
type adminAPIHandlers struct {
}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(mux *router.Router) {

	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := mux.NewRoute().PathPrefix(adminAPIPathPrefix).Subrouter()

	// Version handler
	adminRouter.Methods(http.MethodGet).Path("/version").HandlerFunc(adminAPI.VersionHandler)

	adminV1Router := adminRouter.PathPrefix("/v1").Subrouter()

	/// Service operations

	// Service status
	adminV1Router.Methods(http.MethodGet).Path("/service").HandlerFunc(adminAPI.ServiceStatusHandler)

	// Service restart and stop - TODO
	adminV1Router.Methods(http.MethodPost).Path("/service").HandlerFunc(adminAPI.ServiceStopNRestartHandler)

	// Info operations
	adminV1Router.Methods(http.MethodGet).Path("/info").HandlerFunc(adminAPI.ServerInfoHandler)

	/// Lock operations

	// List Locks
	adminV1Router.Methods(http.MethodGet).Path("/locks").HandlerFunc(adminAPI.ListLocksHandler)
	// Clear locks
	adminV1Router.Methods(http.MethodDelete).Path("/locks").HandlerFunc(adminAPI.ClearLocksHandler)

	/// Heal operations

	// Heal processing endpoint.
	adminV1Router.Methods(http.MethodPost).Path("/heal/").HandlerFunc(adminAPI.HealHandler)
	adminV1Router.Methods(http.MethodPost).Path("/heal/{bucket}").HandlerFunc(adminAPI.HealHandler)
	adminV1Router.Methods(http.MethodPost).Path("/heal/{bucket}/{prefix:.*}").HandlerFunc(adminAPI.HealHandler)

	/// Config operations

	// Update credentials
	adminV1Router.Methods(http.MethodPut).Path("/config/credential").HandlerFunc(adminAPI.UpdateCredentialsHandler)
	// Get config
	adminV1Router.Methods(http.MethodGet).Path("/config").HandlerFunc(adminAPI.GetConfigHandler)
	// Set config
	adminV1Router.Methods(http.MethodPut).Path("/config").HandlerFunc(adminAPI.SetConfigHandler)
}
