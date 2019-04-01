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

	if globalServerConfig.GetRegion() != globalMinioDefaultRegion {
		t.Errorf("Expecting region `us-east-1` found %s", globalServerConfig.GetRegion())
	}

	// Set new region and verify.
	globalServerConfig.SetRegion("us-west-1")
	if globalServerConfig.GetRegion() != "us-west-1" {
		t.Errorf("Expecting region `us-west-1` found %s", globalServerConfig.GetRegion())
	}

	if err := saveServerConfig(context.Background(), objLayer, globalServerConfig); err != nil {
		t.Fatalf("Unable to save updated config file %s", err)
	}

	// Initialize server config.
	if err := loadConfig(objLayer); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}

func TestServerConfigWithEnvs(t *testing.T) {

	os.Setenv("MINIO_BROWSER", "off")
	defer os.Unsetenv("MINIO_BROWSER")

	os.Setenv("MINIO_WORM", "on")
	defer os.Unsetenv("MINIO_WORM")

	os.Setenv("MINIO_ACCESS_KEY", "minio")
	defer os.Unsetenv("MINIO_ACCESS_KEY")

	os.Setenv("MINIO_SECRET_KEY", "minio123")
	defer os.Unsetenv("MINIO_SECRET_KEY")

	os.Setenv("MINIO_REGION", "us-west-1")
	defer os.Unsetenv("MINIO_REGION")

	os.Setenv("MINIO_DOMAIN", "domain.com")
	defer os.Unsetenv("MINIO_DOMAIN")

	defer resetGlobalIsEnvs()

	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)

	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatalf("Init Test config failed")
	}

	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	serverHandleEnvVars()

	// Init config
	initConfig(objLayer)

	// Check if serverConfig has browser disabled
	if globalIsBrowserEnabled {
		t.Error("Expected browser to be disabled but it is not")
	}

	// Check if serverConfig returns WORM config from the env
	if !globalServerConfig.GetWorm() {
		t.Error("Expected WORM to be enabled but it is not")
	}

	// Check if serverConfig has region from the environment
	if globalServerConfig.GetRegion() != "us-west-1" {
		t.Errorf("Expected region to be \"us-west-1\", found %v", globalServerConfig.GetRegion())
	}

	// Check if serverConfig has credentials from the environment
	cred := globalServerConfig.GetCredential()

	if cred.AccessKey != "minio" {
		t.Errorf("Expected access key to be `minio`, found %s", cred.AccessKey)
	}

	if cred.SecretKey != "minio123" {
		t.Errorf("Expected access key to be `minio123`, found %s", cred.SecretKey)
	}

	// Check if serverConfig has the correct domain
	if globalDomainNames[0] != "domain.com" {
		t.Errorf("Expected Domain to be `domain.com`, found " + globalDomainNames[0])
	}
}
