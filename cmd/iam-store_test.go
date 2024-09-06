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

package cmd

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/minio/pkg/v3/policy"
)

func fatalIf(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIAMStore_AddUsersToGroup(t *testing.T) {
	ctx := context.Background()

	objLayer, _, err := prepareFS(ctx)
	fatalIf(t, err)
	store := &IAMStoreSys{
		IAMStorageAPI: newIAMObjectStore(objLayer, MinIOUsersSysType),
	}

	_, err = store.AddUsersToGroup(ctx, "testGroup", []string{})
	fatalIf(t, err)

	// Set a random policy, doesn't matter for this test
	p, err := policy.ParseConfig(strings.NewReader(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::mybucket/*"}]}`))
	fatalIf(t, err)
	_, err = store.SetPolicy(ctx, "testPolicy", *p)
	fatalIf(t, err)

	_, err = store.PolicyDBSet(ctx, "testGroup", "testPolicy", regUser, true)
	fatalIf(t, err)

	groups, err := store.ListGroups(ctx)
	fatalIf(t, err)
	wantGroups := []string{"testGroup"}
	if !slices.Equal(groups, wantGroups) {
		t.Fatalf("Expected groups: %v, got: %v", wantGroups, groups)
	}
}
