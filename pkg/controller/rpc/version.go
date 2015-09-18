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

	"github.com/minio/minio/pkg/version"
)

// Args basic json RPC params
type Args struct {
	Request string
}

// VersionReply version reply
type VersionReply struct {
	Version   string `json:"version"`
	BuildDate string `json:"buildDate"`
}

// VersionService -
type VersionService struct{}

func getVersion() string {
	return "0.0.1"
}

func getBuildDate() string {
	return version.Version
}

func setVersionReply(reply *VersionReply) {
	reply.Version = getVersion()
	reply.BuildDate = getBuildDate()
	return
}

// Get method
func (v *VersionService) Get(r *http.Request, args *Args, reply *VersionReply) error {
	setVersionReply(reply)
	return nil
}
