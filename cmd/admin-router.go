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

import router "github.com/gorilla/mux"

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
	adminRouter.Methods("GET").Path("/version").HandlerFunc(adminAPI.VersionHandler)

	adminV1Router := mux.NewRoute().PathPrefix("/minio/admin/v1").Subrouter()

	/// Service operations

	// Service status
	adminV1Router.Methods("GET").Path("/service").HandlerFunc(adminAPI.ServiceStatusHandler)

	// Service restart and stop - TODO
	adminV1Router.Methods("POST").Path("/service").HandlerFunc(adminAPI.ServiceStopNRestartHandler)

	// Info operations
	adminV1Router.Methods("GET").Path("/info").HandlerFunc(adminAPI.ServerInfoHandler)

	/// Lock operations

	// List Locks
	adminV1Router.Methods("GET").Path("/locks").HandlerFunc(adminAPI.ListLocksHandler)
	// Clear locks
	adminV1Router.Methods("DELETE").Path("/locks").HandlerFunc(adminAPI.ClearLocksHandler)

	// /// Heal operations

	// // List Objects needing heal.
	// adminRouter.Methods("GET").Queries("heal", "").Headers(minioAdminOpHeader, "list-objects").HandlerFunc(adminAPI.ListObjectsHealHandler)
	// // List Uploads needing heal.
	// adminRouter.Methods("GET").Queries("heal", "").Headers(minioAdminOpHeader, "list-uploads").HandlerFunc(adminAPI.ListUploadsHealHandler)
	// // List Buckets needing heal.
	// adminRouter.Methods("GET").Queries("heal", "").Headers(minioAdminOpHeader, "list-buckets").HandlerFunc(adminAPI.ListBucketsHealHandler)

	// // Heal Buckets.
	// adminRouter.Methods("POST").Queries("heal", "").Headers(minioAdminOpHeader, "bucket").HandlerFunc(adminAPI.HealBucketHandler)
	// // Heal Objects.
	// adminRouter.Methods("POST").Queries("heal", "").Headers(minioAdminOpHeader, "object").HandlerFunc(adminAPI.HealObjectHandler)
	// // Heal Format.
	// adminRouter.Methods("POST").Queries("heal", "").Headers(minioAdminOpHeader, "format").HandlerFunc(adminAPI.HealFormatHandler)
	// // Heal Uploads.
	// adminRouter.Methods("POST").Queries("heal", "").Headers(minioAdminOpHeader, "upload").HandlerFunc(adminAPI.HealUploadHandler)

	/// Config operations

	// Update credentials
	adminV1Router.Methods("POST").Path("/config/credential").HandlerFunc(adminAPI.UpdateCredentialsHandler)
	// Get config
	adminV1Router.Methods("GET").Path("/config").HandlerFunc(adminAPI.GetConfigHandler)
	// Set config
	adminV1Router.Methods("POST").Path("/config").HandlerFunc(adminAPI.SetConfigHandler)
}
