// +build testrunmain

/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"

	minio "github.com/minio/minio/cmd"
	_ "github.com/minio/minio/cmd/gateway"
)

// TestRunMain takes arguments from APP_ARGS env variable and calls minio.Main(args)
// 1. Build and RUN test executable:
// $ go test -tags testrunmain -covermode count -coverpkg="./..." -c -tags testrunmain
// $ APP_ARGS="server /tmp/test" ./minio.test -test.run "^TestRunMain$" -test.coverprofile coverage.cov
//
// 1. As an alternative you can also run the system under test by just by calling "go test"
// $ APP_ARGS="server /tmp/test" go test -cover -tags testrunmain -covermode count -coverpkg="./..." -coverprofile=coverage.cov
//
// 2. Run System-Tests (when using GitBash prefix this line with MSYS_NO_PATHCONV=1)
//    Note the the SERVER_ENDPOINT must be reachable from inside the docker container (so don't use localhost!)
// $ podman run -e MINT_MODE=full -e SERVER_ENDPOINT=192.168.47.11:9000 -e ACCESS_KEY=minioadmin -e SECRET_KEY=minioadmin -v /tmp/mint/log:/mint/log minio/mint
//
// 3. Stop system under test by sending SIGTERM
// $ ctrl+c
//
// 4. Optionally transform coverage file to HTML
// $ go tool cover -html=./coverage.cov -o coverage.html
//
// 5. Optionally transform the coverage file to .csv
// $ cat coverage.cov | sed -E 's/mode: .*/source;from;to;stmnts;count/g' | sed -E 's/:| |,/;/g' > coverage.csv
func TestRunMain(t *testing.T) {
	cancelChan := make(chan os.Signal, 1)
	// catch SIGETRM or SIGINTERRUPT. The test must gracefully end to complete the test coverage.
	signal.Notify(cancelChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		// start minio server with params from env variable APP_ARGS
		args := os.Getenv("APP_ARGS")
		if args == "" {
			log.Printf("No environment variable APP_ARGS found. Starting minio without parameters ...")
		} else {
			log.Printf("Starting \"minio %v\" ...", args)
		}
		minio.Main(strings.Split("minio.test "+args, " "))
	}()
	sig := <-cancelChan
	log.Printf("Caught SIGTERM %v", sig)
	log.Print("You might want to transform the coverage.cov file to .html by calling:")
	log.Print("$ go tool cover -html=./coverage.cov -o coverage.html")
	// shutdown other goroutines gracefully
	// close other resources
}
