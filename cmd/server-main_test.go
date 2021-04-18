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
	"reflect"
	"testing"
)

// Tests initializing new object layer.
func TestNewObjectLayer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Tests for FS object layer.
	nDisks := 1
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}
	defer removeRoots(disks)

	obj, err := newObjectLayer(ctx, mustGetPoolEndpoints(disks...))
	if err != nil {
		t.Fatal("Unexpected object layer initialization error", err)
	}
	_, ok := obj.(*FSObjects)
	if !ok {
		t.Fatal("Unexpected object layer detected", reflect.TypeOf(obj))
	}

	// Tests for Erasure object layer initialization.

	// Create temporary backend for the test server.
	nDisks = 16
	disks, err = getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Failed to create disks for the backend")
	}
	defer removeRoots(disks)

	obj, err = newObjectLayer(ctx, mustGetPoolEndpoints(disks...))
	if err != nil {
		t.Fatal("Unexpected object layer initialization error", err)
	}

	_, ok = obj.(*erasureServerPools)
	if !ok {
		t.Fatal("Unexpected object layer detected", reflect.TypeOf(obj))
	}
}
