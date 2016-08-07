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

func (h healHandler) ListObjects(arg *HealListArgs, reply *HealListReply) error {
	info, err := h.ObjectAPI.ListObjectsHeal(arg.Bucket, arg.Prefix, arg.Marker, arg.Delimiter, arg.MaxKeys)
	if err != nil {
		return err
	}
	reply.IsTruncated = info.IsTruncated
	reply.NextMarker = info.NextMarker
	for _, obj := range info.Objects {
		reply.Objects = append(reply.Objects, obj.Name)
	}
	return nil
}

type HealObjectArgs struct {
	Bucket string
	Object string
}

type HealObjectReply struct{}

func (h healHandler) HealObject(arg *HealObjectArgs, reply *HealObjectReply) error {
	return h.ObjectAPI.HealObject(arg.Bucket, arg.Object)
}
