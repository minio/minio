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
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"

	"github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/probe"
)

type controllerRPCService struct {
	serverList []ServerRep
}

// generateAuth generate new auth keys for a user
func generateAuth(args *AuthArgs, reply *AuthRep) *probe.Error {
	config, err := LoadConfig()
	if err != nil {
		if os.IsNotExist(err.ToGoError()) {
			// Initialize new config, since config file doesn't exist yet
			config = &AuthConfig{}
			config.Version = "0.0.1"
			config.Users = make(map[string]*AuthUser)
		} else {
			return err.Trace()
		}
	}
	if _, ok := config.Users[args.User]; ok {
		return probe.NewError(errors.New("Credentials already set, if you wish to change this invoke Reset() method"))
	}
	accessKeyID, err := generateAccessKeyID()
	if err != nil {
		return err.Trace()
	}
	reply.AccessKeyID = string(accessKeyID)

	secretAccessKey, err := generateSecretAccessKey()
	if err != nil {
		return err.Trace()
	}
	reply.SecretAccessKey = string(secretAccessKey)
	reply.Name = args.User

	config.Users[args.User] = &AuthUser{
		Name:            args.User,
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	if err := SaveConfig(config); err != nil {
		return err.Trace()
	}
	return nil
}

// fetchAuth fetch auth keys for a user
func fetchAuth(args *AuthArgs, reply *AuthRep) *probe.Error {
	config, err := LoadConfig()
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
	config, err := LoadConfig()
	if err != nil {
		return err.Trace()
	}
	if _, ok := config.Users[args.User]; !ok {
		return probe.NewError(errors.New("User not found"))
	}
	accessKeyID, err := generateAccessKeyID()
	if err != nil {
		return err.Trace()
	}
	reply.AccessKeyID = string(accessKeyID)
	secretAccessKey, err := generateSecretAccessKey()
	if err != nil {
		return err.Trace()
	}
	reply.SecretAccessKey = string(secretAccessKey)
	reply.Name = args.User

	config.Users[args.User] = &AuthUser{
		Name:            args.User,
		AccessKeyID:     string(accessKeyID),
		SecretAccessKey: string(secretAccessKey),
	}
	return SaveConfig(config).Trace()
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

func proxyRequest(method, host string, ssl bool, res interface{}) *probe.Error {
	u := &url.URL{}
	if ssl {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}
	u.Host = host
	if _, _, err := net.SplitHostPort(host); err == nil {
		u.Host = host
	} else {
		u.Host = host + ":9002"
	}
	u.Path = "/rpc"

	op := rpcOperation{
		Method:  method,
		Request: ServerArg{},
	}
	request, err := newRPCRequest(u.String(), op, nil)
	if err != nil {
		return err.Trace()
	}
	var resp *http.Response
	resp, err = request.Do()
	if err != nil {
		return err.Trace()
	}
	if err := json.DecodeClientResponse(resp.Body, res); err != nil {
		return probe.NewError(err)
	}
	return nil
}

// StorageStats returns dummy storage stats
func (s *controllerRPCService) StorageStats(r *http.Request, args *ControllerArgs, reply *StorageStatsRep) error {
	err := proxyRequest("Donut.StorageStats", args.Host, args.SSL, reply)
	if err != nil {
		return probe.WrapError(err)
	}
	return nil
}

// RebalaceStats returns dummy rebalance stats
func (s *controllerRPCService) RebalanceStats(r *http.Request, args *ControllerArgs, reply *RebalanceStatsRep) error {
	err := proxyRequest("Donut.RebalanceStats", args.Host, args.SSL, reply)
	if err != nil {
		return probe.WrapError(err)
	}
	return nil
}

func (s *controllerRPCService) AddServer(r *http.Request, args *ControllerArgs, res *ServerRep) error {
	err := proxyRequest("Server.Add", args.Host, args.SSL, res)
	if err != nil {
		return probe.WrapError(err)
	}
	s.serverList = append(s.serverList, *res)
	return nil
}

func (s *controllerRPCService) DiscoverServers(r *http.Request, args *DiscoverArgs, rep *DiscoverRep) error {
	c := make(chan DiscoverRepEntry)
	defer close(c)
	for _, host := range args.Hosts {
		go func(c chan DiscoverRepEntry, host string) {
			u := &url.URL{}
			if args.SSL {
				u.Scheme = "https"
			} else {
				u.Scheme = "http"
			}
			if args.Port != 0 {
				u.Host = host + ":" + string(args.Port)
			} else {
				u.Host = host + ":9002"
			}
			u.Path = "/rpc"

			op := rpcOperation{
				Method:  "Server.Version",
				Request: ServerArg{},
			}
			versionrep := VersionRep{}
			request, err := newRPCRequest(u.String(), op, nil)
			if err != nil {
				c <- DiscoverRepEntry{host, err.ToGoError().Error()}
				return
			}
			var resp *http.Response
			resp, err = request.Do()
			if err != nil {
				c <- DiscoverRepEntry{host, err.ToGoError().Error()}
				return
			}
			if err := json.DecodeClientResponse(resp.Body, &versionrep); err != nil {
				c <- DiscoverRepEntry{host, err.Error()}
				return
			}
			c <- DiscoverRepEntry{host, ""}
		}(c, host)
	}
	for range args.Hosts {
		entry := <-c
		rep.Entry = append(rep.Entry, entry)
	}
	return nil
}

func (s *controllerRPCService) GetControllerNetInfo(r *http.Request, args *ServerArg, res *ControllerNetInfoRep) error {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}
	for _, addr := range addrs {
		res.NetInfo = append(res.NetInfo, addr.String())
	}
	return nil
}

func (s *controllerRPCService) GetServerMemStats(r *http.Request, args *ControllerArgs, res *MemStatsRep) error {
	err := proxyRequest("Server.MemStats", args.Host, args.SSL, res)
	if err != nil {
		return probe.WrapError(err)
	}
	return nil
}

func (s *controllerRPCService) GetServerDiskStats(r *http.Request, args *ControllerArgs, res *DiskStatsRep) error {
	err := proxyRequest("Server.DiskStats", args.Host, args.SSL, res)
	if err != nil {
		return probe.WrapError(err)
	}
	return nil
}

func (s *controllerRPCService) GetServerSysInfo(r *http.Request, args *ControllerArgs, res *SysInfoRep) error {
	err := proxyRequest("Server.SysInfo", args.Host, args.SSL, res)
	if err != nil {
		return probe.WrapError(err)
	}
	return nil
}

func (s *controllerRPCService) ListServers(r *http.Request, args *ControllerArgs, res *ListRep) error {
	res.List = s.serverList
	return nil
}

func (s *controllerRPCService) GetServerVersion(r *http.Request, args *ControllerArgs, res *VersionRep) error {
	err := proxyRequest("Server.Version", args.Host, args.SSL, res)
	if err != nil {
		return probe.WrapError(err)
	}
	return nil
}

func (s *controllerRPCService) GetVersion(r *http.Request, args *ControllerArgs, res *VersionRep) error {
	res.Version = "0.0.1"
	res.BuildDate = minioVersion
	res.Architecture = runtime.GOARCH
	res.OperatingSystem = runtime.GOOS
	return nil
}
