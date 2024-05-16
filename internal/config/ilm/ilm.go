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

package ilm

import (
	"strconv"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

// DefaultKVS default configuration values for ILM subsystem
var DefaultKVS = config.KVS{
	config.KV{
		Key:   transitionWorkers,
		Value: "100",
	},
	config.KV{
		Key:   expirationWorkers,
		Value: "100",
	},
}

// Config represents the different configuration values for ILM subsystem
type Config struct {
	TransitionWorkers int
	ExpirationWorkers int
}

// LookupConfig - lookup ilm config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	cfg = Config{
		TransitionWorkers: 100,
		ExpirationWorkers: 100,
	}

	if err = config.CheckValidKeys(config.ILMSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	tw, err := strconv.Atoi(env.Get(EnvILMTransitionWorkers, kvs.GetWithDefault(transitionWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	ew, err := strconv.Atoi(env.Get(EnvILMExpirationWorkers, kvs.GetWithDefault(expirationWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	cfg.TransitionWorkers = tw
	cfg.ExpirationWorkers = ew
	return cfg, nil
}
