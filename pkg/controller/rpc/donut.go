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

	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/probe"
)

const tb = (1024 * 1024 * 1024 * 1024)

// DonutService donut service
type DonutService struct{}

// DonutArgs collections of disks and name to initialize donut
type DonutArgs struct {
	Name     string
	MaxSize  uint64
	Hostname string
	Disks    []string
}

// Reply reply for successful or failed Set operation
type Reply struct {
	Message string `json:"message"`
	Error   error  `json:"error"`
}

func setDonut(args *DonutArgs, reply *Reply) *probe.Error {
	conf := &donut.Config{Version: "0.0.1"}
	conf.DonutName = args.Name
	conf.MaxSize = args.MaxSize
	conf.NodeDiskMap = make(map[string][]string)
	conf.NodeDiskMap[args.Hostname] = args.Disks
	if err := donut.SaveConfig(conf); err != nil {
		return err.Trace()
	}
	reply.Message = "success"
	reply.Error = nil
	return nil
}

// Set method
func (s *DonutService) Set(r *http.Request, args *DonutArgs, reply *Reply) error {
	if err := setDonut(args, reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}

type BucketStorage struct {
	Name string `json:"name"`
	Used uint64 `json:"used"`
}

type StorageStatsReply struct {
	Buckets []BucketStorage `json:"buckets"`
}

func (s *DonutService) StorageStats(r *http.Request, args *DonutArgs, reply *StorageStatsReply) error {
	reply.Buckets = []BucketStorage{{"bucket1", 4 * tb}, {"bucket2", 120 * tb}, {"bucket3", 45 * tb}}
	return nil
}

type RebalanceStatsReply struct {
	Inprogress []string `json:"inprogress"`
	Done       []string `json:"done"`
}

func (s *DonutService) RebalaceStats(r *http.Request, args *DonutArgs, reply *RebalanceStatsReply) error {
	reply.Inprogress = []string{"bucket1/obj1", "bucket2/obj2", "bucket3/obj3"}
	reply.Done = []string{"bucket1/rebobj1", "bucket2/rebobj2", "bucket3/rebobj3"}
	return nil
}
