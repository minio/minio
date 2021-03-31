/*
*
*  Mint, (C) 2021 Minio, Inc.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software

*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
 */

package main

import (
	"os"
	"time"

	"github.com/minio/mc/cmd"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/pkg/madmin"
)

// mc admin client for testing
var adminClient *madmin.AdminClient

func main() {
	function := "admin-main"

	configureLog()

	var err *probe.Error
	adminClient, err = createAdminClient()
	if err != nil {
		failureLog(function, map[string]interface{}{}, time.Now(), "", "Could not create admin client", err.Cause).Fatal()
		return
	}

	testSubnetHealthUsage()
}

func createAdminClient() (*madmin.AdminClient, *probe.Error) {
	endpoint := os.Getenv("SERVER_ENDPOINT")
	accessKey := os.Getenv("ACCESS_KEY")
	secretKey := os.Getenv("SECRET_KEY")
	secure := os.Getenv("ENABLE_HTTPS") == "1"
	sdkEndpoint := "http://" + endpoint
	if secure {
		sdkEndpoint = "https://" + endpoint
	}

	admConfig := new(cmd.Config)

	admConfig.AppName = "mint-tests-admin"
	admConfig.AppVersion = cmd.ReleaseTag
	admConfig.Debug = true
	admConfig.Insecure = !secure

	admConfig.HostURL = sdkEndpoint
	admConfig.AccessKey = accessKey
	admConfig.SecretKey = secretKey
	admConfig.Signature = "s3v4"
	admConfig.Lookup = minio.BucketLookupAuto

	return cmd.NewAdminFactory()(admConfig)
}
