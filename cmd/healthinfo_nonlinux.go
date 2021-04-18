// +build !linux

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
	"context"
	"net/http"
	"runtime"

	"github.com/minio/minio/pkg/madmin"
)

func getLocalDiskHwInfo(ctx context.Context, r *http.Request) madmin.ServerDiskHwInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = globalLocalNodeName
	}

	return madmin.ServerDiskHwInfo{
		Addr:  addr,
		Error: "unsupported platform: " + runtime.GOOS,
	}
}

func getLocalOsInfo(ctx context.Context, r *http.Request) madmin.ServerOsInfo {
	addr := r.Host
	if globalIsDistErasure {
		addr = globalLocalNodeName
	}

	return madmin.ServerOsInfo{
		Addr:  addr,
		Error: "unsupported platform: " + runtime.GOOS,
	}
}
