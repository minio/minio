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

package cmd

import (
	"bytes"
	"crypto/rand"
	"os"
	"path"
	"testing"

	"github.com/minio/cli"
)

// Test to call healControl() in control-heal-main.go
func TestControlHealMain(t *testing.T) {
	// create cli app for testing
	app := cli.NewApp()
	app.Commands = []cli.Command{controlCmd}

	// start test server
	testServer := StartTestServer(t, "XL")

	// schedule cleanup at the end
	defer testServer.Stop()

	// fetch http server endpoint
	url := testServer.Server.URL

	// create args to call
	args := []string{"./minio", "control", "heal", url}

	// run app
	err := app.Run(args)
	if err != nil {
		t.Errorf("Control-Heal-Format-Main test failed with - %s", err.Error())
	}

	obj := newObjectLayerFn()
	// Create "bucket"
	err = obj.MakeBucket("bucket")
	if err != nil {
		t.Fatal(err)
	}

	bucket := "bucket"
	object := "object"

	data := make([]byte, 1*1024*1024)
	length := int64(len(data))
	_, err = rand.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	_, err = obj.PutObject(bucket, object, length, bytes.NewReader(data), nil, "")
	if err != nil {
		t.Fatal(err)
	}

	// Remove the object - to simulate the case where the disk was down when the object was created.
	err = os.RemoveAll(path.Join(testServer.Disks[0].path, bucket, object))
	if err != nil {
		t.Fatal(err)
	}

	args = []string{"./minio", "control", "heal", url + "/bucket"}
	// run app
	err = app.Run(args)
	if err != nil {
		t.Errorf("Control-Heal-Bucket-Main test failed with - %s", err.Error())
	}

	args = []string{"./minio", "control", "heal", url + "/bucket/object"}
	// run app
	err = app.Run(args)
	if err != nil {
		t.Errorf("Control-Heal-Bucket-With-Prefix-Main test failed with - %s", err.Error())
	}
}

// Test to call lockControl() in control-lock-main.go
func TestControlLockMain(t *testing.T) {
	// Create cli app for testing
	app := cli.NewApp()
	app.Commands = []cli.Command{controlCmd}

	// Start test server
	testServer := StartTestServer(t, "XL")

	// Schedule cleanup at the end
	defer testServer.Stop()

	// Fetch http server endpoint
	url := testServer.Server.URL

	// Create args to call
	args := []string{"./minio", "control", "lock", "list", url}

	// Run app
	err := app.Run(args)
	if err != nil {
		t.Errorf("Control-Lock-Main test failed with - %s", err.Error())
	}
}

// Test to call serviceControl(stop) in control-service-main.go
func TestControlServiceStopMain(t *testing.T) {
	// create cli app for testing
	app := cli.NewApp()
	app.Commands = []cli.Command{controlCmd}

	// Initialize done channel specifically for each tests.
	globalServiceDoneCh = make(chan struct{}, 1)
	// Initialize signal channel specifically for each tests.
	globalServiceSignalCh = make(chan serviceSignal, 1)

	// start test server
	testServer := StartTestServer(t, "XL")

	// schedule cleanup at the end
	defer testServer.Stop()

	// fetch http server endpoint
	url := testServer.Server.URL

	// create args to call
	args := []string{"./minio", "control", "service", "stop", url}

	// run app
	err := app.Run(args)
	if err != nil {
		t.Errorf("Control-Service-Stop-Main test failed with - %s", err)
	}
}

// Test to call serviceControl(status) in control-service-main.go
func TestControlServiceStatusMain(t *testing.T) {
	// create cli app for testing
	app := cli.NewApp()
	app.Commands = []cli.Command{controlCmd}

	// Initialize done channel specifically for each tests.
	globalServiceDoneCh = make(chan struct{}, 1)
	// Initialize signal channel specifically for each tests.
	globalServiceSignalCh = make(chan serviceSignal, 1)

	// start test server
	testServer := StartTestServer(t, "XL")

	// schedule cleanup at the end
	defer testServer.Stop()

	// fetch http server endpoint
	url := testServer.Server.URL

	// Create args to call
	args := []string{"./minio", "control", "service", "status", url}

	// run app
	err := app.Run(args)
	if err != nil {
		t.Errorf("Control-Service-Status-Main test failed with - %s", err)
	}

	// Create args to call
	args = []string{"./minio", "control", "service", "stop", url}

	// run app
	err = app.Run(args)
	if err != nil {
		t.Errorf("Control-Service-Stop-Main test failed with - %s", err)
	}
}

// Test to call serviceControl(restart) in control-service-main.go
func TestControlServiceRestartMain(t *testing.T) {
	// create cli app for testing
	app := cli.NewApp()
	app.Commands = []cli.Command{controlCmd}

	// Initialize done channel specifically for each tests.
	globalServiceDoneCh = make(chan struct{}, 1)
	// Initialize signal channel specifically for each tests.
	globalServiceSignalCh = make(chan serviceSignal, 1)

	// start test server
	testServer := StartTestServer(t, "XL")

	// schedule cleanup at the end
	defer testServer.Stop()

	// fetch http server endpoint
	url := testServer.Server.URL

	// Create args to call
	args := []string{"./minio", "control", "service", "restart", url}

	// run app
	err := app.Run(args)
	if err != nil {
		t.Errorf("Control-Service-Restart-Main test failed with - %s", err)
	}

	// Initialize done channel specifically for each tests.
	globalServiceDoneCh = make(chan struct{}, 1)
	// Initialize signal channel specifically for each tests.
	globalServiceSignalCh = make(chan serviceSignal, 1)

	// Create args to call
	args = []string{"./minio", "control", "service", "stop", url}

	// run app
	err = app.Run(args)
	if err != nil {
		t.Errorf("Control-Service-Stop-Main test failed with - %s", err)
	}
}

// NOTE: This test practically always passes, but its the only way to
// execute mainControl in a test situation
func TestControlMain(t *testing.T) {
	// create cli app for testing
	app := cli.NewApp()
	app.Commands = []cli.Command{controlCmd}

	// create args to call
	args := []string{"./minio", "control"}

	// run app
	err := app.Run(args)
	if err != nil {
		t.Errorf("Control-Main test failed with - %s", err)
	}
}
