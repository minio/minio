/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/probe"
)

// HealObjectHandler - handler to invoke healing of objects or object.
func (capi controllerAPIHandlers) HealObjectHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]
	_, isRecursive := r.URL.Query()["recursive"]

	switch getRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		writeErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case authTypeSigned:
		if s3Error := isReqAuthenticated(r); s3Error != ErrNone {
			writeErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// Initialize done channel to close the heal object routine.
	doneCh := make(chan struct{})

	// Close when the call returns.
	defer close(doneCh)

	// Heal an object or all objects.
	for healObjInfo := range capi.ControllerAPI.HealObject(bucket, object, isRecursive, doneCh) {
		healObjInfoBytes, e := json.Marshal(healObjInfo)
		if e != nil {
			errorIf(probe.NewError(e), "Unable to marshal heal info into json.", nil)
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(healObjInfoBytes)
	}
	w.(http.Flusher).Flush()
}
