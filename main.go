package main

import (
	"log"
	"os"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkg/server"
)

func getStorageType(input string) server.StorageType {
	switch {
	case input == "file":
		return server.FileStorage
	case input == "inmemory":
		return server.InMemoryStorage
	default:
		{
			log.Println("Unknown storage type:", input)
			log.Println("Choosing default storage type as 'file'..")
			return server.FileStorage
		}
	}
}

func runCmd(c *cli.Context) {
	storageTypeStr := c.String("storage-type")
	apiaddress := c.String("api-address")
	webaddress := c.String("web-address")
	certFile := c.String("cert")
	keyFile := c.String("key")
	if (certFile != "" && keyFile == "") || (certFile == "" && keyFile != "") {
		log.Fatal("Both certificate and key must be provided to enable https")
	}
	tls := (certFile != "" && keyFile != "")
	storageType := getStorageType(storageTypeStr)
	var serverConfigs []server.ServerConfig
	apiServerConfig := server.ServerConfig{
		Address:  apiaddress,
		Tls:      tls,
		CertFile: certFile,
		KeyFile:  keyFile,
		ApiType: server.MinioApi{
			StorageType: storageType,
		},
	}
	webUiServerConfig := server.ServerConfig{
		Address:  webaddress,
		Tls:      false,
		CertFile: "",
		KeyFile:  "",
		ApiType: server.WebUIApi{
			Websocket: false,
		},
	}
	serverConfigs = append(serverConfigs, apiServerConfig)
	serverConfigs = append(serverConfigs, webUiServerConfig)
	server.Start(serverConfigs)
}

func main() {
	app := cli.NewApp()
	app.Name = "minio"
	app.Usage = ""
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "api-address,a",
			Value: ":8080",
			Usage: "address for incoming API requests",
		},
		cli.StringFlag{
			Name:  "web-address,w",
			Value: ":8081",
			Usage: "address for incoming Management UI requests",
		},
		cli.StringFlag{
			Name:  "cert,c",
			Value: "",
			Usage: "cert.pem",
		},
		cli.StringFlag{
			Name:  "key,k",
			Value: "",
			Usage: "key.pem",
		},
		cli.StringFlag{
			Name:  "storage-type,s",
			Value: "file",
			Usage: "valid entries: file,inmemory",
		},
	}
	app.Action = runCmd
	app.Run(os.Args)
}
