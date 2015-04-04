/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"path"

	"github.com/gorilla/mux"
	"github.com/minio-io/iodine"
	"github.com/minio-io/minio/pkg/api/config"
	"github.com/minio-io/minio/pkg/utils/crypto/keys"
	"github.com/minio-io/minio/pkg/utils/log"
)

const (
	defaultWeb = "polygon"
)

type webAPI struct {
	conf    config.Config
	webPath string
}

// No encoder interface exists, so we create one.
type encoder interface {
	Encode(v interface{}) error
}

// HTTPHandler - http wrapper handler
func HTTPHandler() http.Handler {
	mux := mux.NewRouter()
	var api = webAPI{}

	if err := api.conf.SetupConfig(); err != nil {
		log.Fatal(iodine.New(err, nil))
	}

	api.webPath = path.Join(api.conf.GetConfigPath(), defaultWeb)
	mux.Handle("/{polygon:.*}", http.FileServer(http.Dir(api.webPath))).Methods("GET")
	mux.HandleFunc("/access", api.accessHandler).Methods("POST")
	return mux
}

func writeResponse(w http.ResponseWriter, response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	var encoder encoder
	w.Header().Set("Content-Type", "application/json")
	encoder = json.NewEncoder(&bytesBuffer)
	w.Header().Set("Server", "Minio Management Console")
	w.Header().Set("Connection", "close")
	encoder.Encode(response)
	return bytesBuffer.Bytes()
}

func (web *webAPI) accessHandler(w http.ResponseWriter, req *http.Request) {
	var err error
	var accesskey, secretkey []byte
	username := req.FormValue("username")
	if len(username) <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = web.conf.ReadConfig()
	if err != nil {
		log.Error.Println(iodine.New(err, nil))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	if web.conf.IsUserExists(username) {
		w.WriteHeader(http.StatusConflict)
		return
	}

	var user = config.User{}
	user.Name = username

	accesskey, err = keys.GenerateRandomAlphaNumeric(keys.MinioAccessID)
	if err != nil {
		log.Error.Println(iodine.New(err, nil))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	user.AccessKey = string(accesskey)

	secretkey, err = keys.GenerateRandomBase64(keys.MinioSecretID)
	if err != nil {
		log.Error.Println(iodine.New(err, nil))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	user.SecretKey = string(secretkey)

	web.conf.AddUser(user)
	err = web.conf.WriteConfig()
	if err != nil {
		log.Error.Println(iodine.New(err, nil))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	err = web.conf.ReadConfig()
	if err != nil {
		log.Error.Println(iodine.New(err, nil))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	// Get user back for sending it over HTTP reply
	user = web.conf.GetUser(username)
	w.Write(writeResponse(w, user))
}
