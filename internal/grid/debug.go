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

package grid

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

//go:generate stringer -type=debugMsg $GOFILE

// debugMsg is a debug message for testing purposes.
// may only be used for tests.
type debugMsg int

const debugPrint = false

const (
	debugShutdown debugMsg = iota
	debugKillInbound
	debugKillOutbound
	debugWaitForExit
	debugSetConnPingDuration
	debugSetClientPingDuration
	debugAddToDeadline
)

// NewTestingManager creates a new grid manager for testing purposes.
func NewTestingManager(ctx context.Context, o ManagerOptions) (*Manager, error) {
	found := false
	if o.AuthRequest == nil {
		return nil, fmt.Errorf("grid: AuthRequest must be set")
	}
	m := &Manager{
		ID:          uuid.New(),
		targets:     make(map[string]*Connection, len(o.Hosts)),
		local:       o.Local,
		authRequest: o.AuthRequest,
	}
	m.handlers.init()
	if ctx == nil {
		ctx = context.Background()
	}
	if len(o.Hosts) == 1 {
		host := o.Hosts[0]
		o.Local = host
		m.targets[host] = newConnection(connectionParams{
			ctx:          ctx,
			id:           m.ID,
			local:        o.Local,
			remote:       host,
			dial:         o.Dialer,
			handlers:     &m.handlers,
			auth:         o.AddAuth,
			blockConnect: o.BlockConnect,
			tlsConfig:    o.TLSConfig,
		})
		return m, nil
	}
	for _, host := range o.Hosts {
		if host == o.Local {
			if found {
				return nil, fmt.Errorf("grid: local host found multiple times")
			}
			found = true
			// No connection to local.
			continue
		}
		m.targets[host] = newConnection(connectionParams{
			ctx:          ctx,
			id:           m.ID,
			local:        o.Local,
			remote:       host,
			dial:         o.Dialer,
			handlers:     &m.handlers,
			auth:         o.AddAuth,
			blockConnect: o.BlockConnect,
			tlsConfig:    o.TLSConfig,
		})
	}
	if !found {
		return nil, fmt.Errorf("grid: local host not found")
	}

	return m, nil
}
