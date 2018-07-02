/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

// SysInfo related consts
const (
	// SysInfo config file carries backend format specific details.
	sysInfoConfigFile = "sysinfo.json"

	// SysInfo config tmp file carries backend format.
	sysInfoConfigFileTmp = "sysinfo.json.tmp"
)

const (
	// Version of the sysInfoMetaV1
	sysInfoMetaVersionV1 = "1"
)

// ServerSystemInfoData holds storage, connections and other
// information of a given server
type ServerSystemInfoData struct {
	CPUs              []string `json:"cpus"`
	Totalmemory       uint64   `json:"totalmemory"`
	Disks             []string `json:"disks"`
	TotalDiskCapacity uint64   `json:"totaldiskcapacity"`
	MACAddresses      []string `json:"macaddresses"`
	Hostname          string   `json:"hostname"`
	OS                string   `json:"os"`
	Platform          string   `json:"platform"`
	KernelVersion     string   `json:"kernelversion"`
	HostID            string   `json:"hostid"`
}

// ServerSystemInfo holds server information result of one node
type ServerSystemInfo struct {
	Error string                `json:"error"`
	Addr  string                `json:"addr"`
	Data  *ServerSystemInfoData `json:"data"`
}

// sysinfo.json currently has the following format:
// {
//   "version": "1",
//   "deploymentid": "XXXXX",
//   "nodessysinfo": [
//   {
//		"cpus": ["XXXX","xxxx"],
//		"totalmemory": XXXX,
//		"disks": [
//		  "XXXX"
//		],
//		"diskcapacity": XXXX,
//		"macaddress": ["XXXX", "xxxx"],
//		"hostname": "XXXX",
//		"os": "XXXX",
//		"platform": "XXX",
//		"kernelversion": "XXXXX",
//		"hostid": "XXXXX"
//	  },
//
// }
// Here "XXXXX" depends on the backend, currently we have "fs" and "xl" implementations.
// formatMetaV1 should be inherited by backend format structs. Please look at format-fs.go
// and format-xl.go for details.

// Ideally we will never have a situation where we will have to change the
// fields of this struct and deal with related migration.
// sysInfoConfig struct is contains the deployment ID
// and the sysinfo details of the individual nodes.
type sysInfoConfig struct {
	Version      string                 `json:"version"`
	DeploymentID string                 `json:"deploymentid"`
	NodesSysInfo []ServerSystemInfoData `json:"nodessysinfo"`
}

func newSysInfo(peerLen int) *sysInfoConfig {
	sysInfo := &sysInfoConfig{}
	sysInfo.Version = sysInfoMetaVersionV1
	sysInfo.DeploymentID = mustGetUUID()
	sysInfo.NodesSysInfo = make([]ServerSystemInfoData, peerLen)
	return sysInfo
}
