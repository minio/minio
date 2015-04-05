/*
 * Minimalist Object Storage, (C) 2014,2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"log"
	"os"
	"strings"

	"text/tabwriter"
	"text/template"

	"github.com/minio-io/cli"
	"github.com/minio-io/donut"
)

var infoTemplate = `
{{range $donutName, $nodes := .}}
DONUTNAME: {{$donutName}}
{{range $nodeName, $disks := $nodes}}
NODE: {{$nodeName}}
  DISKS: {{$disks}}
{{end}}
{{end}}
`

var infoPrinter = func(templ string, data interface{}) {
	funcMap := template.FuncMap{
		"join": strings.Join,
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)
	t := template.Must(template.New("help").Funcs(funcMap).Parse(templ))
	err := t.Execute(w, data)
	if err != nil {
		panic(err)
	}
	w.Flush()
}

// doInfoDonutCmd
func doInfoDonutCmd(c *cli.Context) {
	if !c.Args().Present() {
		log.Fatalln("no args?")
	}
	if len(c.Args()) != 1 {
		log.Fatalln("invalid number of args")
	}
	donutName := c.Args().First()
	if !isValidDonutName(donutName) {
		log.Fatalln("Invalid donutName")
	}
	mcDonutConfigData, err := loadDonutConfig()
	if err != nil {
		log.Fatalln(err)
	}
	if _, ok := mcDonutConfigData.Donuts[donutName]; !ok {
		log.Fatalln("donut does not exist")
	}
	d, err := donut.NewDonut(donutName, getNodeMap(mcDonutConfigData.Donuts[donutName].Node))
	if err != nil {
		log.Fatalln(err)
	}
	donutNodes := make(map[string]map[string][]string)
	donutNodes[donutName], err = d.Info()
	if err != nil {
		log.Fatalln(err)
	}
	infoPrinter(infoTemplate, donutNodes)
}
