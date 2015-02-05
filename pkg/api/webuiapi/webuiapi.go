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
	"log"
	"net/http"
	"path"

	"github.com/gorilla/mux"
	"github.com/minio-io/minio/pkg/utils/config"
	"github.com/minio-io/minio/pkg/utils/crypto/keys"
)

const (
	DEFAULT_WEB = "polygon"
)

type webUiApi struct {
	conf    config.Config
	webPath string
}

func HttpHandler() http.Handler {
	mux := mux.NewRouter()
	var api = webUiApi{}

	if err := api.conf.SetupConfig(); err != nil {
		log.Fatal(err)
	}

	api.webPath = path.Join(api.conf.GetConfigPath(), DEFAULT_WEB)
	mux.Handle("/{polygon:.*}", http.FileServer(http.Dir(api.webPath))).Methods("GET")
	mux.HandleFunc("/access", api.accessHandler).Methods("POST")
	return mux
}

func (web *webUiApi) accessHandler(w http.ResponseWriter, req *http.Request) {
	var err error
	var accesskey, secretkey []byte
	username := req.FormValue("username")
	if len(username) <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var user = config.User{}
	user.Name = username

	accesskey, err = keys.GetRandomAlphaNumeric(keys.MINIO_ACCESS_ID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	user.AccessKey = string(accesskey)

	secretkey, err = keys.GetRandomBase64(keys.MINIO_SECRET_ID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	user.SecretKey = string(secretkey)

	err = web.conf.ReadConfig()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	web.conf.AddUser(user)
	err = web.conf.WriteConfig()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	err = web.conf.ReadConfig()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	log.Println("Config:", web.conf.Users)
}
