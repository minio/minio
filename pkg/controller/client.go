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

package controller

import (
	"encoding/json"
	"net/http"

	jsonrpc "github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/server/rpc"
)

// GetDisks get disks info of the server at given url
func GetDisks(url string) ([]string, error) {
	op := RPCOps{
		Method:  "DiskInfo.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := NewRequest(url, op, http.DefaultTransport)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	resp, err := req.Do()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	defer resp.Body.Close()
	var reply rpc.DiskInfoReply
	if err := jsonrpc.DecodeClientResponse(resp.Body, &reply); err != nil {
		return nil, iodine.New(err, nil)
	}
	return reply.Disks, nil
}

// GetMemStats get memory status of the server at given url
func GetMemStats(url string) ([]byte, error) {
	op := RPCOps{
		Method:  "MemStats.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := NewRequest(url, op, http.DefaultTransport)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	resp, err := req.Do()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	defer resp.Body.Close()
	var reply rpc.MemStatsReply
	if err := jsonrpc.DecodeClientResponse(resp.Body, &reply); err != nil {
		return nil, iodine.New(err, nil)
	}
	return json.MarshalIndent(reply, "", "\t")
}

// GetSysInfo get system status of the server at given url
func GetSysInfo(url string) ([]byte, error) {
	op := RPCOps{
		Method:  "SysInfo.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := NewRequest(url, op, http.DefaultTransport)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	resp, err := req.Do()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	defer resp.Body.Close()
	var reply rpc.SysInfoReply
	if err := jsonrpc.DecodeClientResponse(resp.Body, &reply); err != nil {
		return nil, iodine.New(err, nil)
	}
	return json.MarshalIndent(reply, "", "\t")
}

// SetDonut - set donut config
func SetDonut(url, hostname string, disks []string) error {
	op := RPCOps{
		Method: "Donut.Set",
		Request: rpc.DonutArgs{
			Hostname: hostname,
			Disks:    disks,
			Name:     "default",
			MaxSize:  512000000,
		},
	}
	req, err := NewRequest(url, op, http.DefaultTransport)
	if err != nil {
		return iodine.New(err, nil)
	}
	resp, err := req.Do()
	if err != nil {
		return iodine.New(err, nil)
	}
	defer resp.Body.Close()
	var reply rpc.Reply
	if err := jsonrpc.DecodeClientResponse(resp.Body, &reply); err != nil {
		return iodine.New(err, nil)
	}
	return reply.Error
}

// Add more functions here for other RPC messages
