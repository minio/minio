// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
	app.HideVersion = true
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .VisibleFlags}}[FLAGS]{{end}} METAFILES...
{{if .VisibleFlags}}
GLOBAL FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
`

	app.HideHelpCommand = true

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Usage: "Print each file as a separate line without formatting",
			Name:  "ndjson",
		},
	}

	app.Action = func(c *cli.Context) error {
		if !c.Args().Present() {
			cli.ShowAppHelp(c)
			return nil
		}
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
