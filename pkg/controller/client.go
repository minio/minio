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

package controller

import (
	"encoding/json"
	"net/http"

	jsonrpc "github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/server/rpc"
)

func closeResp(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
}

// GetMemStats get memory status of the server at given url
func GetMemStats(url string) ([]byte, *probe.Error) {
	op := RPCOps{
		Method:  "MemStats.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := NewRequest(url, op, http.DefaultTransport)
	if err != nil {
		return nil, err.Trace()
	}
	resp, err := req.Do()
	defer closeResp(resp)
	if err != nil {
		return nil, err.Trace()
	}
	var reply rpc.MemStatsReply
	if err := jsonrpc.DecodeClientResponse(resp.Body, &reply); err != nil {
		return nil, probe.New(err)
	}
	{
		jsonRespBytes, err := json.MarshalIndent(reply, "", "\t")
		if err != nil {
			return nil, probe.New(err)
		}
		return jsonRespBytes, nil
	}
}

// GetSysInfo get system status of the server at given url
func GetSysInfo(url string) ([]byte, error) {
	op := RPCOps{
		Method:  "SysInfo.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := NewRequest(url, op, http.DefaultTransport)
	if err != nil {
		return nil, err.Trace()
	}
	resp, err := req.Do()
	defer closeResp(resp)
	if err != nil {
		return nil, err.Trace()
	}
	var reply rpc.SysInfoReply
	if err := jsonrpc.DecodeClientResponse(resp.Body, &reply); err != nil {
		return nil, probe.New(err)
	}
	{
		jsonRespBytes, err := json.MarshalIndent(reply, "", "\t")
		if err != nil {
			return nil, probe.New(err)
		}
		return jsonRespBytes, nil
	}
}

// GetAuthKeys get access key id and secret access key
func GetAuthKeys(url string) ([]byte, error) {
	op := RPCOps{
		Method:  "Auth.Get",
		Request: rpc.Args{Request: ""},
	}
	req, err := NewRequest(url, op, http.DefaultTransport)
	if err != nil {
		return nil, err.Trace()
	}
	resp, err := req.Do()
	defer closeResp(resp)
	if err != nil {
		return nil, err.Trace()
	}
	var reply rpc.AuthReply
	if err := jsonrpc.DecodeClientResponse(resp.Body, &reply); err != nil {
		return nil, probe.New(err)
	}
	authConfig := &auth.Config{}
	authConfig.Version = "0.0.1"
	authConfig.Users = make(map[string]*auth.User)
	user := &auth.User{}
	user.Name = "testuser"
	user.AccessKeyID = reply.AccessKeyID
	user.SecretAccessKey = reply.SecretAccessKey
	authConfig.Users[reply.AccessKeyID] = user
	if err := auth.SaveConfig(authConfig); err != nil {
		return nil, err.Trace()
	}
	{
		jsonRespBytes, err := json.MarshalIndent(reply, "", "\t")
		if err != nil {
			return nil, probe.New(err)
		}
		return jsonRespBytes, nil
	}
}

// Add more functions here for other RPC messages
