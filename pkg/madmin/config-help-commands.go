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
 *
 */

package madmin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"
	"text/template"

	"github.com/minio/minio/pkg/color"
)

// Help template used by all sub-systems
const Help = `{{colorBlueBold "Key"}}{{"\t"}}{{colorBlueBold "Description"}}
{{colorYellowBold "----"}}{{"\t"}}{{colorYellowBold "----"}}
{{range $key, $value := .}}{{colorCyanBold $key}}{{ "\t" }}{{$value}}
{{end}}`

// HelpEnv template used by all sub-systems
const HelpEnv = `{{colorBlueBold "KeyEnv"}}{{"\t"}}{{colorBlueBold "Description"}}
{{colorYellowBold "----"}}{{"\t"}}{{colorYellowBold "----"}}
{{range $key, $value := .}}{{colorCyanBold $key}}{{ "\t" }}{{$value}}
{{end}}`

var funcMap = template.FuncMap{
	"colorBlueBold":   color.BlueBold,
	"colorYellowBold": color.YellowBold,
	"colorCyanBold":   color.CyanBold,
}

// HelpTemplate - captures config help template
var HelpTemplate = template.Must(template.New("config-help").Funcs(funcMap).Parse(Help))

// HelpEnvTemplate - captures config help template
var HelpEnvTemplate = template.Must(template.New("config-help-env").Funcs(funcMap).Parse(HelpEnv))

// HelpConfigKV - return help for a given sub-system.
func (adm *AdminClient) HelpConfigKV(subSys, key string, envOnly bool) (io.Reader, error) {
	v := url.Values{}
	v.Set("subSys", subSys)
	v.Set("key", key)
	if envOnly {
		v.Set("env", "")
	}

	reqData := requestData{
		relPath:     adminAPIPrefix + "/help-config-kv",
		queryValues: v,
	}

	// Execute GET on /minio/admin/v2/help-config-kv
	resp, err := adm.executeMethod(http.MethodGet, reqData)
	if err != nil {
		return nil, err
	}
	defer closeResponse(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	var help = make(map[string]string)
	d := json.NewDecoder(resp.Body)
	if err = d.Decode(&help); err != nil {
		return nil, err
	}

	var s strings.Builder
	w := tabwriter.NewWriter(&s, 1, 8, 2, ' ', 0)
	if !envOnly {
		err = HelpTemplate.Execute(w, help)
	} else {
		err = HelpEnvTemplate.Execute(w, help)
	}
	if err != nil {
		return nil, err
	}

	w.Flush()
	return strings.NewReader(s.String()), nil
}
