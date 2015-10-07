/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package main

import "github.com/minio/cli"

// Collection of minio flags currently supported
var flags = []cli.Flag{}

var (
	addressFlag = cli.StringFlag{
		Name:  "address",
		Value: ":9000",
		Usage: "ADDRESS:PORT for cloud storage access.",
	}

	addressControllerFlag = cli.StringFlag{
		Name:  "address-controller",
		Hide:  true,
		Value: ":9001",
		Usage: "ADDRESS:PORT for management console access.",
	}

	addressServerRPCFlag = cli.StringFlag{
		Name:  "address-server-rpc",
		Hide:  true,
		Value: ":9002",
		Usage: "ADDRESS:PORT for management console access.",
	}

	ratelimitFlag = cli.IntFlag{
		Name:  "ratelimit",
		Hide:  true,
		Value: 16,
		Usage: "Limit for total concurrent requests: [DEFAULT: 16].",
	}

	anonymousFlag = cli.BoolFlag{
		Name:  "anonymous",
		Hide:  true,
		Usage: "Make server run in anonymous mode where all client connections are accepted.",
	}

	certFlag = cli.StringFlag{
		Name:  "cert",
		Usage: "Provide your domain certificate.",
	}

	keyFlag = cli.StringFlag{
		Name:  "key",
		Usage: "Provide your domain private key.",
	}

	jsonFlag = cli.BoolFlag{
		Name:  "json",
		Usage: "Enable json formatted output.",
	}
)

// registerFlag registers a cli flag
func registerFlag(flag cli.Flag) {
	flags = append(flags, flag)
}
