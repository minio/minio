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
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/donut"
	"github.com/minio/minio/pkg/probe"
)

type controllerRPCService struct {
	serverList []ServerArg
}

func makeDonut(args *DonutArgs, reply *DefaultRep) *probe.Error {
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

// MakeDonut method
func (s *controllerRPCService) MakeDonut(r *http.Request, args *DonutArgs, reply *DefaultRep) error {
	if err := makeDonut(args, reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}

// generateAuth generate new auth keys for a user
func generateAuth(args *AuthArgs, reply *AuthRep) *probe.Error {
	config, err := auth.LoadConfig()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			// Initialize new config, since config file doesn't exist yet
			config = &auth.Config{}
			config.Version = "0.0.1"
			config.Users = make(map[string]*auth.User)
		} else {
			return err.Trace()
		}
	}
	if _, ok := config.Users[args.User]; ok {
		return probe.NewError(errors.New("Credentials already set, if you wish to change this invoke Reset() method"))
	}
	accessKeyID, err := auth.GenerateAccessKeyID()
	if err != nil {
		return err.Trace()
	}
	reply.AccessKeyID = string(accessKeyID)

	secretAccessKey, err := auth.GenerateSecretAccessKey()
	if err != nil {
		return err.Trace()
	}
	reply.SecretAccessKey = string(secretAccessKey)
	reply.Name = args.User

	config.Users[args.User] = &auth.User{
		Name:            args.User,
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	if err := auth.SaveConfig(config); err != nil {
		return err.Trace()
	}
	return nil
}

// fetchAuth fetch auth keys for a user
func fetchAuth(args *AuthArgs, reply *AuthRep) *probe.Error {
	config, err := auth.LoadConfig()
	if err != nil {
		return err.Trace()
	}
	if _, ok := config.Users[args.User]; !ok {
		return probe.NewError(errors.New("User not found"))
	}
	reply.AccessKeyID = config.Users[args.User].AccessKeyID
	reply.SecretAccessKey = config.Users[args.User].SecretAccessKey
	reply.Name = args.User
	return nil
}

// resetAuth reset auth keys for a user
func resetAuth(args *AuthArgs, reply *AuthRep) *probe.Error {
	config, err := auth.LoadConfig()
	if err != nil {
		return err.Trace()
	}
	if _, ok := config.Users[args.User]; !ok {
		return probe.NewError(errors.New("User not found"))
	}
	accessKeyID, err := auth.GenerateAccessKeyID()
	if err != nil {
		return err.Trace()
	}
	reply.AccessKeyID = string(accessKeyID)
	secretAccessKey, err := auth.GenerateSecretAccessKey()
	if err != nil {
		return err.Trace()
	}
	reply.SecretAccessKey = string(secretAccessKey)
	reply.Name = args.User

	config.Users[args.User] = &auth.User{
		Name:            args.User,
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	return auth.SaveConfig(config).Trace()
}

// Generate auth keys
func (s *controllerRPCService) GenerateAuth(r *http.Request, args *AuthArgs, reply *AuthRep) error {
	if strings.TrimSpace(args.User) == "" {
		return errors.New("Invalid argument")
	}
	if err := generateAuth(args, reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}

// Fetch auth keys
func (s *controllerRPCService) FetchAuth(r *http.Request, args *AuthArgs, reply *AuthRep) error {
	if strings.TrimSpace(args.User) == "" {
		return errors.New("Invalid argument")
	}
	if err := fetchAuth(args, reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}

// Reset auth keys, generates new set of auth keys
func (s *controllerRPCService) ResetAuth(r *http.Request, args *AuthArgs, reply *AuthRep) error {
	if strings.TrimSpace(args.User) == "" {
		return errors.New("Invalid argument")
	}
	if err := resetAuth(args, reply); err != nil {
		return probe.WrapError(err)
	}
	return nil
}

func proxyRequest(method string, url string, arg interface{}, res interface{}) error {
	// can be configured to something else in future
	op := rpcOperation{
		Method:  method,
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

func (s *controllerRPCService) AddServer(r *http.Request, arg *ServerArg, res *DefaultRep) error {
	err := proxyRequest("Server.Add", arg.URL, arg, res)
	if err == nil {
		s.serverList = append(s.serverList, *arg)
	}
	return err
}

func (s *controllerRPCService) GetServerMemStats(r *http.Request, arg *ServerArg, res *MemStatsRep) error {
	return proxyRequest("Server.MemStats", arg.URL, arg, res)
}

func (s *controllerRPCService) GetServerDiskStats(r *http.Request, arg *ServerArg, res *DiskStatsRep) error {
	return proxyRequest("Server.DiskStats", arg.URL, arg, res)
}

func (s *controllerRPCService) GetServerSysInfo(r *http.Request, arg *ServerArg, res *SysInfoRep) error {
	return proxyRequest("Server.SysInfo", arg.URL, arg, res)
}

func (s *controllerRPCService) ListServers(r *http.Request, arg *ServerArg, res *ListRep) error {
	res.List = s.serverList
	return nil
}

func (s *controllerRPCService) GetServerVersion(r *http.Request, arg *ServerArg, res *VersionRep) error {
	return proxyRequest("Server.Version", arg.URL, arg, res)
}
