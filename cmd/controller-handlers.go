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

// ListObjects - list all objects that needs healing.
func (c *controllerAPIHandlers) ListObjectsHeal(arg *HealListArgs, reply *HealListReply) error {
	objAPI := c.ObjectAPI
	if objAPI == nil {
		return errInvalidArgument
	}
	info, err := objAPI.ListObjectsHeal(arg.Bucket, arg.Prefix, arg.Marker, arg.Delimiter, arg.MaxKeys)
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
func (c *controllerAPIHandlers) HealObject(arg *HealObjectArgs, reply *HealObjectReply) error {
	objAPI := c.ObjectAPI
	if objAPI == nil {
		return errInvalidArgument
	}
	return objAPI.HealObject(arg.Bucket, arg.Object)
}

// ShutdownArgs - argument for Shutdown RPC.
type ShutdownArgs struct {
	Reboot bool
}

// ShutdownReply - reply by Shutdown RPC.
type ShutdownReply struct{}

// Shutdown - Shutdown the server.

func (c *controllerAPIHandlers) Shutdown(arg *ShutdownArgs, reply *ShutdownReply) error {
	if arg.Reboot {
		globalShutdownSignalCh <- shutdownRestart
	} else {
		globalShutdownSignalCh <- shutdownHalt
	}
	return nil
}
