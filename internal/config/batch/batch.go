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
	"github.com/minio/pkg/v3/env"
)

// Batch job environment variables
const (
	ReplicationWorkersWait = "replication_workers_wait"
	KeyRotationWorkersWait = "keyrotation_workers_wait"
	ExpirationWorkersWait  = "expiration_workers_wait"

	EnvReplicationWorkersWait   = "MINIO_BATCH_REPLICATION_WORKERS_WAIT"
	EnvKeyRotationWorkersWait   = "MINIO_BATCH_KEYROTATION_WORKERS_WAIT"
	EnvKeyExpirationWorkersWait = "MINIO_BATCH_EXPIRATION_WORKERS_WAIT"
)

var configMu sync.RWMutex

// Config represents the batch job settings.
type Config struct {
	ReplicationWorkersWait time.Duration `json:"replicationWorkersWait"`
	KeyRotationWorkersWait time.Duration `json:"keyRotationWorkersWait"`
	ExpirationWorkersWait  time.Duration `json:"expirationWorkersWait"`
}

// ExpirationWait returns the duration for which a batch expiration worker
// would wait before working on next object.
func (opts Config) ExpirationWait() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()

	return opts.ExpirationWorkersWait
}

// ReplicationWait returns the duration for which a batch replication worker
// would wait before working on next object.
func (opts Config) ReplicationWait() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()

	return opts.ReplicationWorkersWait
}

// KeyRotationWait returns the duration for which a batch key-rotation worker
// would wait before working on next object.
func (opts Config) KeyRotationWait() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()

	return opts.KeyRotationWorkersWait
}

// Clone returns a copy of Config value
func (opts Config) Clone() Config {
	configMu.RLock()
	defer configMu.RUnlock()

	return Config{
		ReplicationWorkersWait: opts.ReplicationWorkersWait,
		KeyRotationWorkersWait: opts.KeyRotationWorkersWait,
		ExpirationWorkersWait:  opts.ExpirationWorkersWait,
	}
}

// Update updates opts with nopts
func (opts *Config) Update(nopts Config) {
	configMu.Lock()
	defer configMu.Unlock()

	opts.ReplicationWorkersWait = nopts.ReplicationWorkersWait
	opts.KeyRotationWorkersWait = nopts.KeyRotationWorkersWait
	opts.ExpirationWorkersWait = nopts.ExpirationWorkersWait
}

// DefaultKVS - default KV config for batch job settings
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
	if err = config.CheckValidKeys(config.BatchSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	cfg.ReplicationWorkersWait = 0
	cfg.KeyRotationWorkersWait = 0
	cfg.ExpirationWorkersWait = 0

	rduration, err := time.ParseDuration(env.Get(EnvReplicationWorkersWait, kvs.GetWithDefault(ReplicationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if rduration < 0 {
		return cfg, config.ErrInvalidBatchReplicationWorkersWait(nil)
	}

	kduration, err := time.ParseDuration(env.Get(EnvKeyRotationWorkersWait, kvs.GetWithDefault(KeyRotationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if kduration < 0 {
		return cfg, config.ErrInvalidBatchKeyRotationWorkersWait(nil)
	}

	eduration, err := time.ParseDuration(env.Get(EnvKeyExpirationWorkersWait, kvs.GetWithDefault(ExpirationWorkersWait, DefaultKVS)))
	if err != nil {
		return cfg, err
	}
	if eduration < 0 {
		return cfg, config.ErrInvalidBatchExpirationWorkersWait(nil)
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
