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

// Network properties of a server
type Network struct {
	IP       string `json:"address"`
	Mask     string `json:"netmask"`
	Ethernet string `json:"networkInterface"`
}

// ServerArg server metadata to identify a server
type ServerArg struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	ID   string `json:"id"`
}

// ServerRep server reply container for Server.List
type ServerRep struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	ID      string `json:"id"`
}

// DefaultRep default reply
type DefaultRep struct {
	Error   int64  `json:"error"`
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
	List []ServerArg `json:"list"`
}

// VersionRep version reply
type VersionRep struct {
	Version         string `json:"version"`
	BuildDate       string `json:"buildDate"`
	Architecture    string `json:"arch"`
	OperatingSystem string `json:"os"`
}
