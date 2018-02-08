/*
 * Minio Cloud Storage, (C) 2016,2017 Minio, Inc.
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

/*
 * Below main package has canonical imports for 'go get' and 'go build'
 * to work with all other clones of github.com/minio/minio repository. For
 * more information refer https://golang.org/doc/go1.4#canonicalimports
 */

package main // import "github.com/minio/minio"

import (
	"fmt"
	"os"
	"runtime"

	version "github.com/hashicorp/go-version"
	"github.com/minio/mc/pkg/console"
	minio "github.com/minio/minio/cmd"

	// Import gateway
	_ "github.com/minio/minio/cmd/gateway"
)

const (
	// Minio requires at least Go v1.9.4
	minGoVersion        = "1.9.4"
	goVersionConstraint = ">= " + minGoVersion
)

// Check if this binary is compiled with at least minimum Go version.
func checkGoVersion(goVersionStr string) error {
	constraint, err := version.NewConstraint(goVersionConstraint)
	if err != nil {
		return fmt.Errorf("'%s': %s", goVersionConstraint, err)
	}

	goVersion, err := version.NewVersion(goVersionStr)
	if err != nil {
		return err
	}

	if !constraint.Check(goVersion) {
		return fmt.Errorf("Minio is not compiled by Go %s.  Please recompile accordingly",
			goVersionConstraint)
	}

	return nil
}

func main() {
	// When `go get` is used minimum Go version check is not triggered but it would have compiled it successfully.
	// However such binary will fail at runtime, hence we also check Go version at runtime.
	if err := checkGoVersion(runtime.Version()[2:]); err != nil {
		console.Fatalln("Go runtime version check failed.", err)
	}

	minio.Main(os.Args)
}
