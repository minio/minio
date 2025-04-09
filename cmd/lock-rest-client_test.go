// Copyright (c) 2015-2021 MinIO, Inc.
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
	"context"
	"testing"

	"github.com/minio/minio/internal/dsync"
)

// Tests lock rpc client.
func TestLockRESTlient(t *testing.T) {
	// These should not be connectable.
	endpoint, err := NewEndpoint("http://localhost:9876")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	endpointLocal, err := NewEndpoint("http://localhost:9012")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	endpointLocal.IsLocal = true

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err = initGlobalLockGrid(ctx, []PoolEndpoints{{Endpoints: Endpoints{endpoint, endpointLocal}}})
	if err != nil {
		t.Fatal(err)
	}

	lkClient := newlockRESTClient(endpoint)
	if lkClient.IsOnline() {
		t.Fatalf("unexpected result. connection was online")
	}

	// Attempt all calls.
	_, err = lkClient.RLock(t.Context(), dsync.LockArgs{})
	if err == nil {
		t.Fatal("Expected for Rlock to fail")
	}

	_, err = lkClient.Lock(t.Context(), dsync.LockArgs{})
	if err == nil {
		t.Fatal("Expected for Lock to fail")
	}

	_, err = lkClient.RUnlock(t.Context(), dsync.LockArgs{})
	if err == nil {
		t.Fatal("Expected for RUnlock to fail")
	}

	_, err = lkClient.Unlock(t.Context(), dsync.LockArgs{})
	if err == nil {
		t.Fatal("Expected for Unlock to fail")
	}
}
