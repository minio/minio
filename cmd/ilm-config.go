// Copyright (c) 2015-2024 MinIO, Inc.
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
	"sync"

	"github.com/minio/minio/internal/config/ilm"
)

var globalILMConfig = ilmConfig{
	cfg: ilm.Config{
		ExpirationWorkers: 100,
		TransitionWorkers: 100,
	},
}

type ilmConfig struct {
	mu  sync.RWMutex
	cfg ilm.Config
}

func (c *ilmConfig) getExpirationWorkers() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cfg.ExpirationWorkers
}

func (c *ilmConfig) getTransitionWorkers() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.cfg.TransitionWorkers
}

func (c *ilmConfig) update(cfg ilm.Config) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cfg = cfg
}
