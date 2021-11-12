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
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/minio/cli"
	"github.com/tinylib/msgp/msgp"
)

func main() {
	app := cli.NewApp()
	app.Copyright = "MinIO, Inc."
	app.Usage = "healing.bin to JSON"
	app.HideVersion = true
	app.HideHelpCommand = true
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .VisibleFlags}}[FLAGS]{{end}} [HEALINGBINFILE|INSPECTZIPFILE]

files ending in '.zip' will be searched for '.healing.bin files recursively and
printed together as a single JSON.

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
`
	app.Flags = []cli.Flag{}
	app.Action = func(c *cli.Context) error {
		if !c.Args().Present() {
			cli.ShowAppHelpAndExit(c, 1) // last argument is exit code
		}

		ht := make(map[string]map[string]interface{})
		file := c.Args().Get(0)
		if strings.HasSuffix(file, ".zip") {
			var sz int64
			f, err := os.Open(file)
			if err != nil {
				return err
			}
			if st, err := f.Stat(); err == nil {
				sz = st.Size()
			}
			defer f.Close()
			zr, err := zip.NewReader(f, sz)
			if err != nil {
				return err
			}
			for _, file := range zr.File {
				if !file.FileInfo().IsDir() && strings.HasSuffix(file.Name, ".healing.bin") {
					r, err := file.Open()
					if err != nil {
						return err
					}

					b, err := ioutil.ReadAll(r)
					if err != nil {
						return err
					}
					buf := bytes.NewBuffer(nil)
					if _, err = msgp.CopyToJSON(buf, bytes.NewReader(b)); err != nil {
						return err
					}

					dec := json.NewDecoder(buf)
					// Use number to preserve integers.
					dec.UseNumber()
					var htr map[string]interface{}
					if err = dec.Decode(&htr); err != nil {
						return err
					}
					ht[file.Name] = htr
				}
			}
			b, err := json.MarshalIndent(ht, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(b))
			return nil
		}
		b, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}
		buf := bytes.NewBuffer(nil)
		if _, err = msgp.CopyToJSON(buf, bytes.NewReader(b)); err != nil {
			return err
		}
		var htr map[string]interface{}
		dec := json.NewDecoder(buf)
		// Use number to preserve integers.
		dec.UseNumber()
		if err = dec.Decode(&htr); err != nil {
			return err
		}
		ht[file] = htr
		b, err = json.MarshalIndent(ht, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
