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

package rpc

import "net/http"

// DonutService donut service
type DonutService struct{}

// DonutArgs collections of disks and name to initialize donut
type DonutArgs struct {
	MaxSize int64
	Name    string
	Disks   []string
}

// Reply reply for successful or failed Set operation
type Reply struct {
	Message string `json:"message"`
	Error   error  `json:"error"`
}

func setDonutArgs(args *DonutArgs, reply *Reply) error {
	return nil
}

// Set method
func (s *DonutService) Set(r *http.Request, args *DonutArgs, reply *Reply) error {
	return setDonutArgs(args, reply)
}
