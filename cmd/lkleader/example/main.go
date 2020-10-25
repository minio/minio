package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/minio/minio/cmd/lkleader"
)

func main() {
	addr := flag.String("addr", "localhost:9001", "The address to listen on.")
	nodesList := flag.String("nodes", "localhost:9001,localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9006,localhost:9007,localhost:9008",
		"Comma separated list of host:port")
	flag.Parse()

	nodes := strings.Split(*nodesList, ",")
	nodes = append(nodes, *addr)

	onLeader := func() error {
		fmt.Println("leader")
		return nil
	}
	onFollower := func() error {
		fmt.Println("me follower")
		return nil
	}

	lead, err := lkleader.New(
		*addr,
		nodes,
		onLeader,
		onFollower,
		lkleader.DefaultElectionTickRange,
		lkleader.DefaultHeartbeatTickRange,
	)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		// Start leader selection process
		if err := lead.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	log.Fatal(http.ListenAndServe(*addr, lead.Router()))
}
