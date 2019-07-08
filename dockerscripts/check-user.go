// +build ignore

/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

import (
	"fmt"
	"log"
	"os"
	"os/user"
	"syscall"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
)

var defaultUserGroup string

func init() {
	username := os.Getenv("MINIO_USERNAME")
	groupname := os.Getenv("MINIO_GROUPNAME")
	defaultUserGroup = username + ":" + groupname
}

func getUserGroup(path string) (string, error) {
	fi, err := os.Stat(minio.PathJoin(path, ".minio.sys"))
	if err != nil {
		// Fresh directory we should default to what was requested by user.
		if os.IsNotExist(err) {
			fi, err = os.Stat(path)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		// Unable to figure out uid/gid, default to defaultUserGroup
		return defaultUserGroup, nil
	}
	u, err := user.LookupId(fmt.Sprintf("%d", stat.Uid))
	if err != nil {
		return fmt.Sprintf("%d:%d", stat.Uid, stat.Gid), nil
	}
	g, err := user.LookupGroupId(fmt.Sprintf("%d", stat.Gid))
	if err != nil {
		return fmt.Sprintf("%d:%d", stat.Uid, stat.Gid), nil
	}
	return fmt.Sprintf("%s:%s", u.Username, g.Name), nil
}

func main() {
	app := cli.NewApp()
	app.Flags = append(minio.ServerFlags, minio.GlobalFlags...)
	app.Action = func(ctx *cli.Context) {
		// Fetch address option
		serverAddr := ctx.GlobalString("address")
		if serverAddr == "" || serverAddr == ":9000" {
			serverAddr = ctx.String("address")
		}
		if ctx.Args().First() == "help" {
			cli.ShowCommandHelpAndExit(ctx, "check-user", 1)
		}
		if ctx.Args().First() != "minio" {
			cli.ShowCommandHelpAndExit(ctx, "check-user", 1)
		}
		args := cli.Args(ctx.Args().Tail())
		if !args.Present() {
			cli.ShowCommandHelpAndExit(ctx, "check-user", 1)
		}
		var ug string
		var err error
		switch args.First() {
		case "gateway":
			args = cli.Args(args.Tail())
			if args.First() != "nas" {
				fmt.Println(defaultUserGroup)
				return
			}
			args = cli.Args(args.Tail())
			if args.First() == "" {
				fmt.Println("")
				return
			}
			ug, err = getUserGroup(args.First())
			if err != nil {
				log.Fatalln(err)
			}
		case "server":
			var setArgs [][]string
			setArgs, err = minio.GetAllSets(args.Tail()...)
			if err != nil {
				log.Fatalln(err)
			}
			var endpoints minio.EndpointList
			_, endpoints, _, err = minio.CreateEndpoints(serverAddr, setArgs...)
			if err != nil {
				log.Fatalln(err)
			}
			for _, endpoint := range endpoints {
				if !endpoint.IsLocal {
					continue
				}
				ug, err = getUserGroup(endpoint.Path)
				if err != nil {
					log.Fatalln(err)
				}
				break
			}
		default:
			cli.ShowCommandHelpAndExit(ctx, "check-user", 1)
		}
		fmt.Println(ug)
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatalln(err)
	}
}
