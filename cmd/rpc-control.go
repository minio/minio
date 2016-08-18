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

package cmd

import (
	"net/rpc"

	router "github.com/gorilla/mux"
)

// Routes paths for "minio control" commands.
const (
	controlRPCPath = reservedBucket + "/control"
	healPath       = controlRPCPath + "/heal"
)

// Register control RPC handlers.
func registerControlRPCRouter(mux *router.Router, objAPI ObjectLayer) {
	healRPCServer := rpc.NewServer()
	healRPCServer.RegisterName("Heal", &healHandler{objAPI})
	mux.Path(healPath).Handler(healRPCServer)
}

// Handler for object healing.
type healHandler struct {
	ObjectAPI ObjectLayer
}

// HealListArgs - argument for ListObjects RPC.
type HealListArgs struct {
	Bucket    string
	Prefix    string
	Marker    string
	Delimiter string
	MaxKeys   int
}

// HealListReply - reply by ListObjects RPC.
type HealListReply struct {
	IsTruncated bool
	NextMarker  string
	Objects     []string
}

// ListObjects - list objects.
func (h healHandler) ListObjects(arg *HealListArgs, reply *HealListReply) error {
	info, err := h.ObjectAPI.ListObjectsHeal(arg.Bucket, arg.Prefix, arg.Marker, arg.Delimiter, arg.MaxKeys)
	if err != nil {
		return err
	}
	reply.IsTruncated = info.IsTruncated
	reply.NextMarker = info.NextMarker
	for _, obj := range info.Objects {
		reply.Objects = append(reply.Objects, obj.Name)
	}
	return nil
}

// HealObjectArgs - argument for HealObject RPC.
type HealObjectArgs struct {
	Bucket string
	Object string
}

// HealObjectReply - reply by HealObject RPC.
type HealObjectReply struct{}

// HealObject - heal the object.
func (h healHandler) HealObject(arg *HealObjectArgs, reply *HealObjectReply) error {
	return h.ObjectAPI.HealObject(arg.Bucket, arg.Object)
}
