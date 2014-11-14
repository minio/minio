package main

import (
	"github.com/codegangsta/cli"
	"github.com/gorilla/mux"
	"github.com/minio-io/minio"
	"log"
	"net/http"
	"os"
)

func main() {
	app := cli.NewApp()
	router := mux.NewRouter()
	runServer := false
	app.Commands = []cli.Command{
		{
			Name:  "storage",
			Usage: "Start a storage node",
			Action: func(c *cli.Context) {
				minio.RegisterStorageHandlers(router)
				runServer = true
			},
		},
		{
			Name:  "gateway",
			Usage: "Start a gateway node",
			Action: func(c *cli.Context) {
				minio.RegisterGatewayHandlers(router, minio.GatewayConfig{
					StorageDriver: minio.InMemoryStorageDriver,
					BucketDriver:  minio.SynchronizedBucketDriver,
				})
				runServer = true
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal("App failed to load", err)
	}
	if runServer {
		log.Fatal(http.ListenAndServe(":8080", router))
	}
}
