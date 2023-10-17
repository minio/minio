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

package batch

import (
	"sync"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
)

// Compression environment variables
const (
	ReplicationWorkersWait = "replication_workers_wait"
	KeyRotationWorkersWait = "keyrotation_workers_wait"
	ExpirationWorkersWait  = "expiration_workers_wait"

	EnvReplicationWorkersWait   = "MINIO_BATCH_REPLICATION_WORKERS_WAIT"
	EnvKeyRotationWorkersWait   = "MINIO_BATCH_KEYROTATION_WORKERS_WAIT"
	EnvKeyExpirationWorkersWait = "MINIO_BATCH_EXPIRATION_WORKERS_WAIT"
)

var configMutex sync.RWMutex

// Config represents the heal settings.
type Config struct {
	ReplicationWorkersWait time.Duration `json:"replicationWorkersWait"`
	KeyRotationWorkersWait time.Duration `json:"keyRotationWorkersWait"`
	ExpirationWorkersWait  time.Duration `json:"expirationWorkersWait"`
}

// Clone returns a copy of Config value
func (opts Config) Clone() Config {
	configMutex.RLock()
	defer configMutex.RUnlock()

	return Config{
		ReplicationWorkersWait: opts.ReplicationWorkersWait,
		KeyRotationWorkersWait: opts.KeyRotationWorkersWait,
		ExpirationWorkersWait:  opts.ExpirationWorkersWait,
	}
}

// Update updates opts with nopts
func (opts *Config) Update(nopts Config) {
	configMutex.Lock()
	defer configMutex.Unlock()

	opts.ReplicationWorkersWait = nopts.ReplicationWorkersWait
	opts.KeyRotationWorkersWait = nopts.KeyRotationWorkersWait
	opts.ExpirationWorkersWait = nopts.ExpirationWorkersWait
}

// DefaultKVS - default KV config for heal settings
var DefaultKVS = config.KVS{
	config.KV{
		Key:   ReplicationWorkersWait,
		Value: "0ms", // No wait by default between each replication attempts.
	},
	config.KV{
		Key:   KeyRotationWorkersWait,
		Value: "0ms", // No wait by default between each key rotation attempts.
	},
	config.KV{
		Key:   ExpirationWorkersWait,
		Value: "0ms", // No wait by default between each expiration attempts.
	},
}

// LookupConfig - lookup config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	if err = config.CheckValidKeys(config.HealSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	cfg.ReplicationWorkersWait = 0
	cfg.KeyRotationWorkersWait = 0
	cfg.ExpirationWorkersWait = 0

	rduration, err := time.ParseDuration(env.Get(EnvReplicationWorkersWait, kvs.GetWithDefault(ReplicationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	kduration, err := time.ParseDuration(env.Get(EnvKeyRotationWorkersWait, kvs.GetWithDefault(KeyRotationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	eduration, err := time.ParseDuration(env.Get(EnvKeyExpirationWorkersWait, kvs.GetWithDefault(ExpirationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	if rduration > 0 {
		cfg.ReplicationWorkersWait = rduration
	}

	if kduration > 0 {
		cfg.KeyRotationWorkersWait = kduration
	}

	if eduration > 0 {
		cfg.ExpirationWorkersWait = eduration
	}

	return cfg, nil
}
