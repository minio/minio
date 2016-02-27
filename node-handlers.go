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
)

// JoinArgs - join arguments.
type JoinArgs struct {
	Username      string `json:"username" form:"username"`
	Password      string `json:"password" form:"password"`
	TargetAddress string `json:"targetAddress"`
}

// JoinRep - join reply.
type JoinRep struct {
	Token         string `json:"token"`
	SourceAddress string `json:"sourceAddress"`
}

// Join - node join handler.
func (node *nodeAPI) Join(r *http.Request, args *JoinArgs, reply *JoinRep) error {
	jwt := initJWT()
	if jwt.Authenticate(args.Username, args.Password) {
		token, err := jwt.GenerateToken(args.Username)
		if err != nil {
			return &json2.Error{Message: err.Cause.Error(), Data: err.String()}
		}
		reply.Token = token
		reply.SourceAddress = ""
		return nil
	}
	return &json2.Error{Message: "Invalid credentials"}
}

// GenCredsArgs generate credentials.
type GenCredsArgs struct {
	Token string `json:"token"`
}

// GenCredsRep generate creds response.
type GenCredsRep struct {
	SourceAddress string `json:"sourceAddress"`
}

func (node *nodeAPI) GenCreds(r *http.Request, args *GenCredsArgs, rep *GenCredsRep) error {
	if !isJWTReqAuthenticated(r) {
		return &json2.Error{Message: "Unauthorized request"}
	}
	return nil
}
