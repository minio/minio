// +build linux darwin dragonfly freebsd netbsd openbsd rumprun

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

package http

import (
	"net"

	"github.com/valyala/tcplisten"
)

var cfg = &tcplisten.Config{
	DeferAccept: true,
	FastOpen:    true,
	// Bump up the soMaxConn value from 128 to 2048 to
	// handle large incoming concurrent requests.
	Backlog: 2048,
}

// Unix listener with special TCP options.
var listen = cfg.NewListener
var fallbackListen = net.Listen
