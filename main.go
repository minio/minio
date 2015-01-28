package main

import (
	"github.com/minio-io/minio/pkg/server"
	"github.com/spf13/cobra"
)

func main() {
	var tls bool
	var inmemory bool
	var address string
	var certFile string
	var keyFile string
	var minioCommand = &cobra.Command{
		Use:   "minio",
		Short: "minio is a minimal object storage system",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			server.Start(address, tls, certFile, keyFile, inmemory)
		},
	}
	minioCommand.PersistentFlags().BoolVarP(&tls, "tls", "t", false, "enable tls")
	minioCommand.PersistentFlags().BoolVarP(&inmemory, "inmemory", "m", false, "in memory object storage")
	minioCommand.PersistentFlags().StringVarP(&address, "http-address", "a", ":8080", "http address")
	minioCommand.PersistentFlags().StringVarP(&certFile, "cert", "c", "", "cert file path")
	minioCommand.PersistentFlags().StringVarP(&keyFile, "key", "k", "", "key file path")
	minioCommand.Execute()
}
