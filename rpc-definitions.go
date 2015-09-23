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

// DonutArg donut params
type DonutArg struct{}

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

// ControllerNetInfoRep array of ip/mask in the form: 172.17.42.1/16
type ControllerNetInfoRep struct {
	NetInfo []string `json:"netinfo"`
}

// DefaultRep default reply
type DefaultRep struct {
	Error   error  `json:"error"`
	Message string `json:"message"`
}

// ServerListRep collection of server replies
type ServerListRep struct {
	List []ServerRep `json:"list"`
}

// DiskStatsRep collection of disks
type DiskStatsRep struct {
	Disks []string `json:"disks"`
}

// MemStatsRep memory statistics of a server
type MemStatsRep struct {
	Total uint64 `json:"total"`
	Free  uint64 `json:"free"`
}

// Network metadata of a server
type Network struct {
	Address  string `json:"address"`
	NetMask  string `json:"netmask"`
	Hostname string `json:"hostname"`
	Ethernet string `json:"networkInterface"`
}

// NetStatsRep network statistics of a server
type NetStatsRep struct {
	Interfaces []Network `json:"interfaces"`
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

// DiscoverArgs array of IP addresses / names to discover
type DiscoverArgs struct {
	Hosts []string `json:"hosts"`
	Port  int      `json:"port"`
	SSL   bool     `json:"bool"`
}

// DiscoverRepEntry : Error is "" if there is no error
type DiscoverRepEntry struct {
	Host  string `json:"host"`
	Error string `json:"error"`
}

// DiscoverRep list of discovered hosts
type DiscoverRep struct {
	Entry []DiscoverRepEntry `json:"entry"`
}

// BucketStats bucket-name and storage used
type BucketStats struct {
	Name string `json:"name"`
	Used uint64 `json:"used"`
}

// StorageStatsRep array of BucketStats
type StorageStatsRep struct {
	Buckets []BucketStats `json:"bucketStats"`
}

// RebalanceStatsRep rebalance information
type RebalanceStatsRep struct {
	State map[string]string `json:"rebalanceState"`
}

// ListNodesRep all nodes part of donut cluster
type ListNodesRep struct {
	Nodes []struct {
		Hostname string `json:"hostname"`
		Address  string `json:"address"`
		ID       string `json:"id"`
	} `json:"nodes"`
}

// DonutVersionRep reply donut on disk format version
type DonutVersionRep struct {
	Version         string `json:"version"`
	Architecture    string `json:"arch"`
	OperatingSystem string `json:"os"`
}
