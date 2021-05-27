// Copyright (c) 2015-2021 MinIO, Inc.
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

package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)

//go:generate msgp -file=$GOFILE -unexported

// Pool represents each pool and its local
// and remote endpoints
type Pool struct {
	Number      int      `json:"number" msg:"n"`
	LocalHost   string   `json:"local,omitempty" msg:"l"`
	LocalDrives []string `json:"drives,omitempty" msg:"d"`
	RemoteHosts []string `json:"remote,omitempty" msg:"r"`
}

type volumeMeta struct {
	Version int             `msg:"v"`
	Token   string          `msg:"t"`
	Pools   map[string]Pool `msg:"ps"`
}

const (
	volumeMetaFilename = minioMetaBucket + SlashSeparator + "volume.meta"
	volumeMetaVersion  = 1
)

func loadVolumeMeta(drives []string) (*volumeMeta, error) {
	vm := &volumeMeta{}
	for _, drive := range drives {
		volumeMetaPath := pathJoin(drive, volumeMetaFilename)
		buf, err := ioutil.ReadFile(volumeMetaPath)
		if err != nil {
			return nil, err
		}
		if len(buf) <= 2 {
			return nil, fmt.Errorf("unknown version for volume.meta refusing to start MinIO service")
		}
		if _, err = vm.UnmarshalMsg(buf[1:]); err != nil {
			return nil, err
		}
		if vm.Version != volumeMetaVersion {
			return nil, fmt.Errorf("unknown version for volume.meta %d, refusing to start MinIO service", vm.Version)
		}
		return vm, nil
	}
	return nil, errInvalidArgument
}

func saveVolumeMeta(endpoints EndpointServerPools) (*volumeMeta, error) {
	vm := &volumeMeta{
		Version: 1,
		Token:   globalActiveCred.Token(),
	}
	poolRemoteHostnames := endpoints.RemoteHostnames()
	poolLocalHostnames := endpoints.LocalHostnames()
	pools := make(map[string]Pool, len(endpoints))
	for i := range endpoints {
		pools["pool"+strconv.Itoa(i+1)] = Pool{
			LocalHost:   poolLocalHostnames[i],
			LocalDrives: endpoints.LocalDrivesPool(i),
			RemoteHosts: poolRemoteHostnames[i],
			Number:      i + 1,
		}
	}
	vm.Pools = pools

	buf := make([]byte, 1, vm.Msgsize()+1)
	buf[0] = volumeMetaVersion
	buf, err := vm.MarshalMsg(buf)
	if err != nil {
		return nil, err
	}

	vmCurrent, err := loadVolumeMeta(endpoints.LocalDrives())
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if os.IsNotExist(err) {
		for _, drive := range endpoints.LocalDrives() {
			volumeMetaPath := pathJoin(drive, volumeMetaFilename)
			// file really does not exist, create it.
			if err = ioutil.WriteFile(volumeMetaPath, buf, os.ModePerm); err != nil {
				return nil, err
			}
		}
		return vm, nil
	}
	return vmCurrent, nil
}
