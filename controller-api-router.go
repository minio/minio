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

package main

import router "github.com/gorilla/mux"

// Controller router prefix.
const (
	controllerPrefix = reservedBucket + "/controller"
)

// controllerAPIHandler implements and provides http handlers for
// Minio controller API.
type controllerAPIHandlers struct {
	ControllerAPI controllerAPI
}

func registerControllerAPIRouter(mux *router.Router, capi controllerAPIHandlers) {
	// Controller API Router.
	crouter := mux.NewRoute().PathPrefix(controllerPrefix).Subrouter()

	/// Controller operations on object.

	// HealObject
	crouter.Methods("GET").Path("/{bucket}/{object:.+}").HandlerFunc(capi.HealObjectHandler)
}
