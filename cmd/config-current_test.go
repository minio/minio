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
	"os"
	"testing"

	"github.com/minio/minio/internal/config"
)

func TestServerConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	objLayer, fsDir, err := prepareFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)

	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatalf("Init Test config failed")
	}

	if globalSite.Region() != globalMinioDefaultRegion {
		t.Errorf("Expecting region `us-east-1` found %s", globalSite.Region())
	}

	// Set new region and verify.
	config.SetRegion(globalServerConfig, "us-west-1")
	site, err := config.LookupSite(
		globalServerConfig[config.SiteSubSys][config.Default],
		globalServerConfig[config.RegionSubSys][config.Default],
	)
	if err != nil {
		t.Fatal(err)
	}
	if site.Region() != "us-west-1" {
		t.Errorf("Expecting region `us-west-1` found %s", globalSite.Region())
	}

	if err := saveServerConfig(t.Context(), objLayer, globalServerConfig); err != nil {
		t.Fatalf("Unable to save updated config file %s", err)
	}

	// Initialize server config.
	if err := loadConfig(objLayer, nil); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}
