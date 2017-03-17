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

// adminAPIHandlers provides HTTP handlers for Minio admin API.
type adminAPIHandlers struct {
}

// registerAdminRouter - Add handler functions for each service REST API routes.
func registerAdminRouter(mux *router.Router) {

	adminAPI := adminAPIHandlers{}
	// Admin router
	adminRouter := mux.NewRoute().PathPrefix("/").Subrouter()

	/// Service operations

	// Service status
	adminRouter.Methods("GET").Queries("service", "").Headers(minioAdminOpHeader, "status").HandlerFunc(adminAPI.ServiceStatusHandler)

	// Service restart
	adminRouter.Methods("POST").Queries("service", "").Headers(minioAdminOpHeader, "restart").HandlerFunc(adminAPI.ServiceRestartHandler)
	// Service update credentials
	adminRouter.Methods("POST").Queries("service", "").Headers(minioAdminOpHeader, "set-credentials").HandlerFunc(adminAPI.ServiceCredentialsHandler)

	// Info operations
	adminRouter.Methods("GET").Queries("info", "").HandlerFunc(adminAPI.ServerInfoHandler)

	/// Lock operations

	// List Locks
	adminRouter.Methods("GET").Queries("lock", "").Headers(minioAdminOpHeader, "list").HandlerFunc(adminAPI.ListLocksHandler)
	// Clear locks
	adminRouter.Methods("POST").Queries("lock", "").Headers(minioAdminOpHeader, "clear").HandlerFunc(adminAPI.ClearLocksHandler)

	/// Heal operations

	// List Objects needing heal.
	adminRouter.Methods("GET").Queries("heal", "").Headers(minioAdminOpHeader, "list-objects").HandlerFunc(adminAPI.ListObjectsHealHandler)
	// List Uploads needing heal.
	adminRouter.Methods("GET").Queries("heal", "").Headers(minioAdminOpHeader, "list-uploads").HandlerFunc(adminAPI.ListUploadsHealHandler)
	// List Buckets needing heal.
	adminRouter.Methods("GET").Queries("heal", "").Headers(minioAdminOpHeader, "list-buckets").HandlerFunc(adminAPI.ListBucketsHealHandler)

	// Heal Buckets.
	adminRouter.Methods("POST").Queries("heal", "").Headers(minioAdminOpHeader, "bucket").HandlerFunc(adminAPI.HealBucketHandler)
	// Heal Objects.
	adminRouter.Methods("POST").Queries("heal", "").Headers(minioAdminOpHeader, "object").HandlerFunc(adminAPI.HealObjectHandler)
	// Heal Format.
	adminRouter.Methods("POST").Queries("heal", "").Headers(minioAdminOpHeader, "format").HandlerFunc(adminAPI.HealFormatHandler)
	// Heal Uploads.
	adminRouter.Methods("POST").Queries("heal", "").Headers(minioAdminOpHeader, "upload").HandlerFunc(adminAPI.HealUploadHandler)

	/// Config operations

	// Get config
	adminRouter.Methods("GET").Queries("config", "").Headers(minioAdminOpHeader, "get").HandlerFunc(adminAPI.GetConfigHandler)
	// Set Config
	adminRouter.Methods("PUT").Queries("config", "").Headers(minioAdminOpHeader, "set").HandlerFunc(adminAPI.SetConfigHandler)
}
