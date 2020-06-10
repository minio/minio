/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
 *
 */

package kv

import (
	"fmt"

	"github.com/minio/minio/pkg/env"
)

const (
	// EnvIAMBackend defines the IAM backend
	EnvIAMBackend = "MINIO_IAM_BACKEND"
)

// IsBackendConfigured returns true if user has configured a backend
func IsBackendConfigured() bool {
	return env.Get(EnvIAMBackend, "") != ""
}

// NewBackend creates a new backend for current configuration
func NewBackend() (Backend, error) {
	return newBackendForName(env.Get(EnvIAMBackend, ""))
}

func newBackendForName(name string) (Backend, error) {
	switch name {
	case "inmemory":
		return NewInmemoryBackend(), nil
	}
	return nil, fmt.Errorf("No backend found for '%s'", name)
}
