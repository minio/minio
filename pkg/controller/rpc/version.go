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
	"runtime"
)

// VersionArgs basic json RPC params
type VersionArgs struct{}

// VersionService get version service
type VersionService struct{}

// VersionReply version reply
type VersionReply struct {
	Version         string `json:"version"`
	BuildDate       string `json:"buildDate"`
	Architecture    string `json:"arch"`
	OperatingSystem string `json:"os"`
}

// Get version
func (v *VersionService) Get(r *http.Request, args *VersionArgs, reply *VersionReply) error {
	reply.Version = "0.0.1"
	//TODO: Better approach needed here to pass global states like version. --ab.
	//	reply.BuildDate = version.Version
	reply.Architecture = runtime.GOARCH
	reply.OperatingSystem = runtime.GOOS
	return nil
}
