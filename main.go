package main

import (
	"log"

	"github.com/minio-io/minio/pkg/server"
	"github.com/spf13/cobra"
)

func main() {
	var tls bool
	var storageTypeStr string
	var address string
	var certFile string
	var keyFile string
	var minioCommand = &cobra.Command{
		Use:   "minio",
		Short: "minio is a minimal object storage system",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			storageType := getStorageType(storageTypeStr)
			serverConfig := server.ServerConfig{
				Address:     address,
				Tls:         tls,
				CertFile:    certFile,
				KeyFile:     keyFile,
				StorageType: storageType,
			}
			server.Start(serverConfig)
		},
	}
	minioCommand.PersistentFlags().BoolVarP(&tls, "tls", "t", false, "enable tls")
	minioCommand.PersistentFlags().StringVarP(&storageTypeStr, "storage-type", "s", "file", "file,inmemory")
	minioCommand.PersistentFlags().StringVarP(&address, "http-address", "a", ":8080", "http address")
	minioCommand.PersistentFlags().StringVarP(&certFile, "cert", "c", "", "cert file path")
	minioCommand.PersistentFlags().StringVarP(&keyFile, "key", "k", "", "key file path")
	minioCommand.Execute()
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
