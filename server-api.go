/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package main

import "github.com/minio/minio/pkg/donut"

// APIOperation container for individual operations read by Ticket Master
type APIOperation struct {
	ProceedCh chan struct{}
}

// MinioAPI container for API and also carries OP (operation) channel
type MinioAPI struct {
	OP    chan APIOperation
	Donut donut.Interface
}

// NewAPI instantiate a new minio API
func NewAPI() MinioAPI {
	// ignore errors for now
	d, err := donut.New()
	if err != nil {
		panic(err)
	}
	return MinioAPI{
		OP:    make(chan APIOperation),
		Donut: d,
	}
}
