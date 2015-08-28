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

package rpc

import (
	"net/http"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/probe"
)

// AuthService auth service
type AuthService struct{}

// AuthReply reply with new access keys and secret ids
type AuthReply struct {
	AccessKeyID     string `json:"accesskey"`
	SecretAccessKey string `json:"secretaccesskey"`
}

func getAuth(reply *AuthReply) *probe.Error {
	accessID, err := auth.GenerateAccessKeyID()
	if err != nil {
		return err.Trace()
	}
	reply.AccessKeyID = string(accessID)
	secretID, err := auth.GenerateSecretAccessKey()
	if err != nil {
		return err.Trace()
	}
	reply.SecretAccessKey = string(secretID)
	return nil
}

// Get auth keys
func (s *AuthService) Get(r *http.Request, args *Args, reply *AuthReply) error {
	if err := getAuth(reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}
