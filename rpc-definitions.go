/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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

//// In memory metadata

//// RPC params

// AuthArgs auth params
type AuthArgs struct {
	User string `json:"user"`
}

// DonutArgs collections of disks and name to initialize donut
type DonutArgs struct {
	Name     string
	MaxSize  uint64
	Hostname string
	Disks    []string
}

// ServerArg server params
type ServerArg struct{}

// ControllerArgs controller params
type ControllerArgs struct {
	Host string `json:"host"` // host or host:port
	SSL  bool   `json:"ssl"`
	ID   string `json:"id"`
}

//// RPC replies

// ServerRep server reply container for Server.List
type ServerRep struct {
	Host string `json:"host"`
	ID   string `json:"id"`
}

// DefaultRep default reply
type DefaultRep struct {
	Error   error  `json:"error"`
	Message string `json:"message"`
}

// ServerListRep collection of server replies
type ServerListRep struct {
	List []ServerRep
}

// DiskStatsRep collection of disks
type DiskStatsRep struct {
	Disks []string
}

// MemStatsRep memory statistics of a server
type MemStatsRep struct {
	Total uint64 `json:"total"`
	Free  uint64 `json:"free"`
}

// Network metadata of a server
type Network struct {
	IP       string `json:"address"`
	NetMask  string `json:"netmask"`
	Hostname string `json:"hostname"`
	Ethernet string `json:"networkInterface"`
}

// NetStatsRep network statistics of a server
type NetStatsRep struct {
	Interfaces []Network
}

// SysInfoRep system information of a server
type SysInfoRep struct {
	Hostname  string `json:"hostname"`
	SysARCH   string `json:"sysArch"`
	SysOS     string `json:"sysOS"`
	SysCPUS   int    `json:"sysNcpus"`
	Routines  int    `json:"goRoutines"`
	GOVersion string `json:"goVersion"`
}

// ListRep all servers list
type ListRep struct {
	List []ServerRep `json:"list"`
}

// VersionRep version reply
type VersionRep struct {
	Version         string `json:"version"`
	BuildDate       string `json:"buildDate"`
	Architecture    string `json:"arch"`
	OperatingSystem string `json:"os"`
}

// AuthRep reply with access keys and secret ids for the user
type AuthRep struct {
	Name            string `json:"name"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}
