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

import "github.com/minio/minio/internal/config"

const (
	transitionWorkers = "transition_workers"
	expirationWorkers = "expiration_workers"
	// EnvILMTransitionWorkers env variable to configure number of transition workers
	EnvILMTransitionWorkers = "MINIO_ILM_TRANSITION_WORKERS"
	// EnvILMExpirationWorkers env variable to configure number of expiration workers
	EnvILMExpirationWorkers = "MINIO_ILM_EXPIRATION_WORKERS"
)

var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help holds configuration keys and their default values for the ILM
	// subsystem
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         transitionWorkers,
			Type:        "number",
			Description: `set the number of transition workers` + defaultHelpPostfix(transitionWorkers),
			Optional:    true,
		},
		config.HelpKV{
			Key:         expirationWorkers,
			Type:        "number",
			Description: `set the number of expiration workers` + defaultHelpPostfix(expirationWorkers),
			Optional:    true,
		},
	}
)
