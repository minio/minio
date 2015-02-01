/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package webuiapi

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

type webUiApi struct {
}

func HttpHandler() http.Handler {
	mux := mux.NewRouter()
	var api = webUiApi{}
	mux.HandleFunc("/", api.homeHandler).Methods("GET")
	/*
		mux.HandleFunc("/{bucket}/", api.listObjectsHandler).Methods("GET")
		mux.HandleFunc("/{bucket}/", api.putBucketHandler).Methods("PUT")
		mux.HandleFunc("/{bucket}/{object:.*}", api.getObjectHandler).Methods("GET")
		mux.HandleFunc("/{bucket}/{object:.*}", api.headObjectHandler).Methods("HEAD")
		mux.HandleFunc("/{bucket}/{object:.*}", api.putObjectHandler).Methods("PUT")
	*/
	return mux
}

func (web *webUiApi) homeHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Server", "Minio Management Console")
	fmt.Fprintln(w, "Welcome!")
}
