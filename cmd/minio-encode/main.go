package main

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/codegangsta/cli"
	"github.com/minio-io/minio/erasure"
)

func main() {
	app := cli.NewApp()
	app.Name = "minio-encode"
	app.Usage = "erasure encode a byte stream"
	app.Action = func(c *cli.Context) {
		erasureParameters, _ := erasure.ValidateParams(10, 5, 8, erasure.VANDERMONDE)

		encoder := erasure.NewEncoder(erasureParameters)
		input, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			log.Fatal("Error reading stdin")
		}
		encodedData, _ := encoder.Encode(input)
		for key, data := range encodedData {
			ioutil.WriteFile("output."+strconv.Itoa(key), data, 0600)
		}
	}
	app.Run(os.Args)
}
