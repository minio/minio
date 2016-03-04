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

import (
	"net/http"

	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio/pkg/probe"
)

// JoinArgs - join arguments.
type JoinArgs struct {
	TargetAddress string `json:"targetAddress"`
	Username      string `json:"username" form:"username"`
	Password      string `json:"password" form:"password"`
}

// JoinRep - join reply.
type JoinRep struct {
	Token         string `json:"token"`
	SourceAddress string `json:"sourceAddress"`
}

// Join - node join handler.
func (node *nodeAPI) Join(r *http.Request, args *JoinArgs, rep *JoinRep) error {
	jwt := initJWT()
	if jwt.Authenticate(args.Username, args.Password) {
		token, err := jwt.GenerateToken(args.Username)
		if err != nil {
			return &json2.Error{Message: err.Cause.Error(), Data: err.String()}
		}
		rep.Token = token
		rep.SourceAddress = r.URL.Host
		return nil
	}
	return &json2.Error{Message: "Invalid credentials"}
}

// GetCredsArgs generate credentials.
type GetCredsArgs struct{}

// GetCredsRep generate creds response.
type GetCredsRep struct {
	AccessKeyID     string `json:"accessKeyID"`
	SecretAccessKey string `json:"secretAccessKey"`
	SourceAddress   string `json:"sourceAddress"`
}

// GetCreds - get credentials handler.
func (node *nodeAPI) GetCreds(r *http.Request, args *GetCredsArgs, rep *GetCredsRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	conf, err := getConfig()
	if err != nil {
		return &json2.Error{Message: probe.WrapError(err).Error()}
	}
	// Generate this once.
	if conf.Credentials.AccessKeyID == defaultAccessKeyID && conf.Credentials.SecretAccessKey == defaultSecretAccessKey {
		conf.Credentials.AccessKeyID = string(mustGenerateAccessKeyID())
		conf.Credentials.SecretAccessKey = string(mustGenerateSecretAccessKey())
		if err = saveConfig(conf); err != nil {
			return &json2.Error{Message: probe.WrapError(err).Error()}
		}
	}
	// Reply back the credentials.
	rep.AccessKeyID = conf.Credentials.AccessKeyID
	rep.SecretAccessKey = conf.Credentials.SecretAccessKey
	rep.SourceAddress = r.URL.Host
	return nil
}
