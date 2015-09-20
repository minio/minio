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

package main

import (
	"net/http"

	"github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/probe"
)

type controllerRPCService struct {
	serverList []ServerArg
}

func proxyRequest(method string, url string, arg interface{}, res interface{}) error {
	// can be configured to something else in future
	namespace := "Server"
	op := rpcOperation{
		Method:  namespace + "." + method,
		Request: arg,
	}
	request, _ := newRPCRequest(url, op, nil)
	resp, err := request.Do()
	if err != nil {
		return probe.WrapError(err)
	}
	decodeerr := json.DecodeClientResponse(resp.Body, res)
	return decodeerr
}

func (s *controllerRPCService) Add(r *http.Request, arg *ServerArg, res *DefaultRep) error {
	err := proxyRequest("Add", arg.URL, arg, res)
	if err == nil {
		s.serverList = append(s.serverList, *arg)
	}
	return err
}

func (s *controllerRPCService) MemStats(r *http.Request, arg *ServerArg, res *MemStatsRep) error {
	return proxyRequest("MemStats", arg.URL, arg, res)
}

func (s *controllerRPCService) DiskStats(r *http.Request, arg *ServerArg, res *DiskStatsRep) error {
	return proxyRequest("DiskStats", arg.URL, arg, res)
}

func (s *controllerRPCService) SysInfo(r *http.Request, arg *ServerArg, res *SysInfoRep) error {
	return proxyRequest("SysInfo", arg.URL, arg, res)
}

func (s *controllerRPCService) List(r *http.Request, arg *ServerArg, res *ListRep) error {
	res.List = s.serverList
	return nil
}

func (s *controllerRPCService) Version(r *http.Request, arg *ServerArg, res *VersionRep) error {
	return proxyRequest("Version", arg.URL, arg, res)
}
