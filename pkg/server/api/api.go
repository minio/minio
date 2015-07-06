/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package api

import "github.com/minio/minio/pkg/donut"

// Operation container for individual operations read by Ticket Master
type Operation struct {
	ProceedCh chan struct{}
}

// Minio container for API and also carries OP (operation) channel
type Minio struct {
	OP    chan Operation
	Donut donut.Interface
}

// New instantiate a new minio API
func New() Minio {
	// ignore errors for now
	d, err := donut.New()
	if err != nil {
		panic(err)
	}
	return Minio{
		OP:    make(chan Operation),
		Donut: d,
	}
}
