package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/cli"
	"github.com/minio/minio/pkg/probe"
)

var joinCmd = cli.Command{
	Name:   "join",
	Usage:  "Join minio cluster.",
	Action: mainJoin,
	CustomHelpTemplate: `NAME:
   minio {{.Name}} - {{.Usage}}

USAGE:
   minio {{.Name}} SERVER ACCESS-KEY SECRET-KEY

EXAMPLES:
   1. Join a new cluster.
      $ minio {{.Name}} 192.168.1.100:9000 WLGDGYAQYIGI833EV05A BYvgJM101sHngl2uzjXS/OBF/aMxAN06JrJ3qJlF

`,
}

func mainJoin(ctx *cli.Context) {
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, "join", 1)
	}
	serverURLParse, e := url.Parse(ctx.Args().Get(0))
	fatalIf(probe.NewError(e), "URL parsing failed.", nil)

	serverURLParse.Path = path.Join("/minio/rpc", minioVersion)
	req, err := newRPCRequest(rpcOperation{
		Method: "Node.Join",
		Request: JoinArgs{
			Username:    ctx.Args().Get(1),
			Password:    ctx.Args().Get(2),
			NodeAddress: "",
		}}, serverURLParse.String())
	fatalIf(err.Trace(), "Failed in initializing new rpc request.", nil)

	client := &http.Client{}
	res, e := client.Do(req)
	fatalIf(probe.NewError(e), "", nil)

	if res.StatusCode != http.StatusOK {
		fatalIf(probe.NewError(errors.New(res.Status)), "Request failed.", nil)
	}

	token := &JoinRep{}
	e = json2.DecodeClientResponse(res.Body, token)
	fatalIf(probe.NewError(e), "Failed decoding json response body.", nil)

	fmt.Println(token)
}
