/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package config

import (
	"text/template"

	"github.com/minio/minio/pkg/color"
)

// HelpKV - implements help messages for keys
// with value as description of the keys.
type HelpKV map[string]string

// Help template used by all sub-systems
const Help = `{{colorBlueBold "Key"}}{{"\t"}}{{colorBlueBold "Description"}}
{{colorYellowBold "----"}}{{"\t"}}{{colorYellowBold "----"}}
{{range $key, $value := .}}{{colorCyanBold $key}}{{ "\t" }}{{$value}}
{{end}}`

var funcMap = template.FuncMap{
	"colorBlueBold":   color.BlueBold,
	"colorYellowBold": color.YellowBold,
	"colorCyanBold":   color.CyanBold,
	"colorGreenBold":  color.GreenBold,
}

// HelpTemplate - captures config help template
var HelpTemplate = template.Must(template.New("config-help").Funcs(funcMap).Parse(Help))

// Region and Worm help is documented in default config
var (
	RegionHelp = HelpKV{
		RegionName: `Region name of this deployment, eg: "us-west-2"`,
		State:      "Indicates if config region is honored or ignored",
		Comment:    "A comment to describe the region setting",
	}

	WormHelp = HelpKV{
		State:   `Indicates if worm is "on" or "off"`,
		Comment: "A comment to describe the worm state",
	}
)
