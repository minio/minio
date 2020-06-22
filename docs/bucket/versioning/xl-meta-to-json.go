package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/minio/cli"
	"github.com/tinylib/msgp/msgp"
)

var xlHeader = [4]byte{'X', 'L', '2', ' '}

func main() {
	app := cli.NewApp()
	app.Copyright = "MinIO, Inc."
	app.Usage = "xl.meta to JSON"
	app.Version = "0.0.1"
	app.HideHelpCommand = true

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Usage: "Print each file as a separate line without formatting",
			Name:  "ndjson",
		},
	}

	app.Action = func(c *cli.Context) error {
		for _, file := range c.Args() {
			var r io.Reader
			switch file {
			case "-":
				r = os.Stdin
			default:
				f, err := os.Open(file)
				if err != nil {
					return err
				}
				defer f.Close()
				r = f
			}

			// Read header
			var tmp [4]byte
			_, err := io.ReadFull(r, tmp[:])
			if err != nil {
				return err
			}
			if !bytes.Equal(tmp[:], xlHeader[:]) {
				return fmt.Errorf("xlMeta: unknown XLv2 header, expected %v, got %v", xlHeader[:4], tmp[:4])
			}
			// Skip version check for now
			_, err = io.ReadFull(r, tmp[:])
			if err != nil {
				return err
			}

			var buf bytes.Buffer
			_, err = msgp.CopyToJSON(&buf, r)
			if err != nil {
				return err
			}
			if c.Bool("ndjson") {
				fmt.Println(buf.String())
				continue
			}
			var msi map[string]interface{}
			dec := json.NewDecoder(&buf)
			// Use number to preserve integers.
			dec.UseNumber()
			err = dec.Decode(&msi)
			if err != nil {
				return err
			}
			b, err := json.MarshalIndent(msi, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(b))
		}
		return nil
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
