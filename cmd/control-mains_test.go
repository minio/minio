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
		t.Errorf("Control-Heal-Main test failed with - %s", err.Error())
	}
}

// Test to call lockControl() in control-lock-main.go
func TestControlLockMain(t *testing.T) {
	// create cli app for testing
	app := cli.NewApp()
	app.Commands = []cli.Command{controlCmd}

	// start test server
	testServer := StartTestServer(t, "XL")

	// schedule cleanup at the end
	defer testServer.Stop()

	// enabling lock instrumentation.
	globalDebugLock = true
	// initializing the locks.
	initNSLock(false)
	// set debug lock info to `nil` so that other tests do not see
	// such modified env settings.
	defer func() {
		globalDebugLock = false
		nsMutex.debugLockMap = nil
	}()

	// fetch http server endpoint
	url := testServer.Server.URL

	// create args to call
	args := []string{"./minio", "control", "lock", url}

	// run app
	err := app.Run(args)
	if err != nil {
		t.Errorf("Control-Lock-Main test failed with - %s", err.Error())
	}
}

// Test to call shutdownControl() in control-shutdown-main.go
func TestControlShutdownMain(t *testing.T) {
	// create cli app for testing
	app := cli.NewApp()
	app.Commands = []cli.Command{controlCmd}

	// start test server
	testServer := StartTestServer(t, "XL")

	// schedule cleanup at the end
	defer testServer.Stop()

	// fetch http server endpoint
	url := testServer.Server.URL

	// create a dummy exit function
	testExitFn := func(exitCode int) {
		if exitCode != int(exitSuccess) {
			t.Errorf("Control-Shutdown-Main test failed - server exited with non-success error code - %d",
				exitCode)
		}
	}

	// initialize the shutdown signal listener
	err := initGracefulShutdown(testExitFn)
	if err != nil {
		t.Fatalf("Control-Shutdown-Main test failed in initGracefulShutdown() - %s",
			err.Error())
	}

	// create args to call
	args := []string{"./minio", "control", "shutdown", url}

	// run app
	err = app.Run(args)
	if err != nil {
		t.Errorf("Control-Shutdown-Main test failed with - %s",
			err.Error())
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
		t.Errorf("Control-Main test failed with - %s",
			err.Error())
	}
}
