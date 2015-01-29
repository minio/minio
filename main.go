package main

import (
	"log"
	"os"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/pkg/server"
)

func main() {
	app := cli.NewApp()
	app.Name = "minio"
	app.Usage = ""
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "http-address,a",
			Value: ":8080",
			Usage: "http address to listen on",
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
		cli.BoolFlag{
			Name:  "tls,t",
			Usage: "http address to listen on",
		},
		cli.StringFlag{
			Name:  "storage-type,s",
			Value: "file",
			Usage: "valid entries: file,inmemory",
		},
	}
	app.Action = func(c *cli.Context) {
		tls := c.Bool("tls")
		storageTypeStr := c.String("storage-type")
		address := c.String("http-address")
		log.Println(address)
		certFile := c.String("cert")
		keyFile := c.String("key")
		storageType := getStorageType(storageTypeStr)
		serverConfig := server.ServerConfig{
			Address:     address,
			Tls:         tls,
			CertFile:    certFile,
			KeyFile:     keyFile,
			StorageType: storageType,
		}
		server.Start(serverConfig)
	}
	app.Run(os.Args)

	//	var minioCommand = &cobra.Command{
	//		Use:   "minio",
	//		Short: "minio is a minimal object storage system",
	//		Long:  "",
	//		Run: func(cmd *cobra.Command, args []string) {
	//			storageType := getStorageType(storageTypeStr)
	//			server.Start(serverConfig)
	//		},
	//	}
	//	minioCommand.PersistentFlags().BoolVarP(&tls, "tls", "t", false, "enable tls")
	//	minioCommand.PersistentFlags().StringVarP(&storageTypeStr, "storage-type", "s", "file", "file,inmemory")
	//	minioCommand.PersistentFlags().StringVarP(&address, "http-address", "a", ":8080", "http address")
	//	minioCommand.PersistentFlags().StringVarP(&certFile, "cert", "c", "", "cert file path")
	//	minioCommand.PersistentFlags().StringVarP(&keyFile, "key", "k", "", "key file path")
	//	minioCommand.Execute()
}

func getStorageType(input string) server.StorageType {
	switch {
	case input == "file":
		return server.InMemoryStorage
	case input == "inmemory":
		return server.InMemoryStorage
	default:
		{
			log.Fatal("Unknown server type:", input)
			// needed for compile, should never return after fatal log msg
			return server.InMemoryStorage
		}
	}
}
