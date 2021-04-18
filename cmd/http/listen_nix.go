// +build linux darwin dragonfly freebsd netbsd openbsd rumprun

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

package http

import (
	"net"

	"github.com/valyala/tcplisten"
)

var cfg = &tcplisten.Config{
	DeferAccept: true,
	FastOpen:    true,
	// Bump up the soMaxConn value from 128 to 4096 to
	// handle large incoming concurrent requests.
	Backlog: 4096,
}

// Unix listener with special TCP options.
var listen = cfg.NewListener
var fallbackListen = net.Listen
