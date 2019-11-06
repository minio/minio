/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

// GOMAXPROCS=10 go test

package dsync

import "testing"

// Tests dsync.New
func TestNew(t *testing.T) {
	nclnts := make([]NetLocker, 33)
	if _, err := New(nclnts); err == nil {
		t.Fatal("Should have failed")
	}

	nclnts = make([]NetLocker, 1)
	if _, err := New(nclnts); err == nil {
		t.Fatal("Should have failed")
	}

	nclnts = make([]NetLocker, 2)
	nds, err := New(nclnts)
	if err != nil {
		t.Fatal("Should pass", err)
	}

	if nds.dquorumReads != 1 {
		t.Fatalf("Unexpected read quorum values expected 1, got %d", nds.dquorumReads)
	}

	if nds.dquorum != 2 {
		t.Fatalf("Unexpected quorum values expected 2, got %d", nds.dquorum)
	}

	nclnts = make([]NetLocker, 3)
	nds, err = New(nclnts)
	if err != nil {
		t.Fatal("Should pass", err)
	}

	if nds.dquorumReads != nds.dquorum {
		t.Fatalf("Unexpected quorum values for odd nodes we expect read %d and write %d quorum to be same", nds.dquorumReads, nds.dquorum)
	}
}
