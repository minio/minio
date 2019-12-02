/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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

package cmd

import (
	"context"
	"os"
	"testing"

	"github.com/minio/minio/cmd/config"
)

func TestServerConfig(t *testing.T) {
	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)

	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatalf("Init Test config failed")
	}

	if globalServerRegion != globalMinioDefaultRegion {
		t.Errorf("Expecting region `us-east-1` found %s", globalServerRegion)
	}

	// Set new region and verify.
	config.SetRegion(globalServerConfig, "us-west-1")
	region, err := config.LookupRegion(globalServerConfig[config.RegionSubSys][config.Default])
	if err != nil {
		t.Fatal(err)
	}
	if region != "us-west-1" {
		t.Errorf("Expecting region `us-west-1` found %s", globalServerRegion)
	}

	if err := saveServerConfig(context.Background(), objLayer, globalServerConfig); err != nil {
		t.Fatalf("Unable to save updated config file %s", err)
	}

	// Initialize server config.
	if err := loadConfig(objLayer); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}
