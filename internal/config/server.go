// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package config

// Opts holds MinIO configuration options
type Opts struct {
	FTP struct {
		Address          string `yaml:"address"`
		PassivePortRange string `yaml:"passive-port-range"`
	} `yaml:"ftp"`
	SFTP struct {
		Address       string `yaml:"address"`
		SSHPrivateKey string `yaml:"ssh-private-key"`
	} `yaml:"sftp"`
}

// ServerConfigVersion struct is used to extract the version
type ServerConfigVersion struct {
	Version string `yaml:"version"`
}

// ServerConfigCommon struct for server config common options
type ServerConfigCommon struct {
	RootUser    string `yaml:"rootUser"`
	RootPwd     string `yaml:"rootPassword"`
	Addr        string `yaml:"address"`
	ConsoleAddr string `yaml:"console-address"`
	CertsDir    string `yaml:"certs-dir"`
	Options     Opts   `yaml:"options"`
}

// ServerConfigV1 represents a MinIO configuration file v1
type ServerConfigV1 struct {
	ServerConfigVersion
	ServerConfigCommon
	Pools [][]string `yaml:"pools"`
}

// ServerConfig represents a MinIO configuration file
type ServerConfig struct {
	ServerConfigVersion
	ServerConfigCommon
	Pools []struct {
		Args          []string `yaml:"args"`
		SetDriveCount uint64   `yaml:"set-drive-count"`
	} `yaml:"pools"`
}
