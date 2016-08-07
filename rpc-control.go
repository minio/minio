package main

import (
	"net/rpc"

	router "github.com/gorilla/mux"
)

const (
	controlRPCPath = reservedBucket + "/control"
	healPath       = controlRPCPath + "/heal"
)

func registerControlRPCRouter(mux *router.Router, objAPI ObjectLayer) {
	healRPCServer := rpc.NewServer()
	healRPCServer.RegisterName("Heal", &healHandler{objAPI})
	mux.Path(healPath).Handler(healRPCServer)
}

type healHandler struct {
	ObjectAPI ObjectLayer
}

type HealListArgs struct {
	Bucket    string
	Prefix    string
	Marker    string
	Delimiter string
	MaxKeys   int
}

type HealListReply struct {
	IsTruncated bool
	NextMarker  string
	Objects     []string
}

func (h healHandler) List(arg *HealListArgs, reply *HealListReply) error {
	return nil
}
