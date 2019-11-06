/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package dsync

import (
	"errors"
	"math"
)

// Dsync represents dsync client object which is initialized with
// authenticated clients, used to initiate lock REST calls.
type Dsync struct {
	// Number of nodes participating in the distributed locking.
	dNodeCount int

	// List of rest client objects, one per lock server.
	restClnts []NetLocker

	// Simple majority based quorum, set to dNodeCount/2+1
	dquorum int

	// Simple quorum for read operations, set to dNodeCount/2
	dquorumReads int
}

// New - initializes a new dsync object with input restClnts.
func New(restClnts []NetLocker) (*Dsync, error) {
	if len(restClnts) < 2 {
		return nil, errors.New("Dsync is not designed for less than 2 nodes")
	} else if len(restClnts) > 32 {
		return nil, errors.New("Dsync is not designed for more than 32 nodes")
	}

	ds := &Dsync{}
	ds.dNodeCount = len(restClnts)

	// With odd number of nodes, write and read quorum is basically the same
	ds.dquorum = int(ds.dNodeCount/2) + 1
	ds.dquorumReads = int(math.Ceil(float64(ds.dNodeCount) / 2.0))

	// Initialize node name and rest path for each NetLocker object.
	ds.restClnts = make([]NetLocker, ds.dNodeCount)
	copy(ds.restClnts, restClnts)

	return ds, nil
}
