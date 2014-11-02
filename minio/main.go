package main

import (
	"github.com/minios/minios"
)

func main() {
	server := minio.Server{}
	server.Start()
}
