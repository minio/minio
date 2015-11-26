/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/minio-xl/pkg/probe"
)

// command specific flags.
var (
	updateFlags = []cli.Flag{
		cli.BoolFlag{
			Name:  "help, h",
			Usage: "Help for update.",
		},
		cli.BoolFlag{
			Name:  "experimental, E",
			Usage: "Check experimental update.",
		},
	}
)

// Check for new software updates.
var updateCmd = cli.Command{
	Name:   "update",
	Usage:  "Check for a new software update.",
	Action: mainUpdate,
	Flags:  updateFlags,
	CustomHelpTemplate: `Name:
   minio {{.Name}} - {{.Usage}}

USAGE:
   minio {{.Name}} [FLAGS]

FLAGS:
  {{range .Flags}}{{.}}
  {{end}}
EXAMPLES:
   1. Check for any new official release.
      $ minio {{.Name}}

   2. Check for any new experimental release.
      $ minio {{.Name}} --experimental
`,
}

// update URL endpoints.
const (
	minioUpdateStableURL       = "https://dl.minio.io:9000/updates/updates.json"
	minioUpdateExperimentalURL = "https://dl.minio.io:9000/updates/experimental.json"
)

// minioUpdates container to hold updates json.
type minioUpdates struct {
	BuildDate string
	Platforms map[string]string
}

// updateMessage container to hold update messages.
type updateMessage struct {
	Status   string `json:"status"`
	Update   bool   `json:"update"`
	Download string `json:"downloadURL"`
	Version  string `json:"version"`
}

// String colorized update message.
func (u updateMessage) String() string {
	if !u.Update {
		updateMessage := color.New(color.FgGreen, color.Bold).SprintfFunc()
		return updateMessage("You are already running the most recent version of ‘minio’.")
	}
	var msg string
	if runtime.GOOS == "windows" {
		msg = "Download " + u.Download
	} else {
		msg = "Download " + u.Download
	}
	msg, err := colorizeUpdateMessage(msg)
	fatalIf(err.Trace(msg), "Unable to colorize experimental update notification string ‘"+msg+"’.", nil)
	return msg
}

// JSON jsonified update message.
func (u updateMessage) JSON() string {
	u.Status = "success"
	updateMessageJSONBytes, err := json.Marshal(u)
	fatalIf(probe.NewError(err), "Unable to marshal into JSON.", nil)

	return string(updateMessageJSONBytes)
}

// verify updates for releases.
func getReleaseUpdate(updateURL string) {
	data, e := http.Get(updateURL)
	fatalIf(probe.NewError(e), "Unable to read from update URL ‘"+updateURL+"’.", nil)

	if minioVersion == "UNOFFICIAL.GOGET" {
		fatalIf(probe.NewError(errors.New("")),
			"Update mechanism is not supported for ‘go get’ based binary builds.  Please download official releases from https://minio.io/#minio", nil)
	}

	current, e := time.Parse(time.RFC3339, minioVersion)
	fatalIf(probe.NewError(e), "Unable to parse version string as time.", nil)

	if current.IsZero() {
		fatalIf(probe.NewError(errors.New("")),
			"Updates not supported for custom builds. Version field is empty. Please download official releases from https://minio.io/#minio", nil)
	}

	var updates minioUpdates
	decoder := json.NewDecoder(data.Body)
	e = decoder.Decode(&updates)
	fatalIf(probe.NewError(e), "Unable to decode update notification.", nil)

	latest, e := time.Parse(time.RFC3339, updates.BuildDate)
	if e != nil {
		latest, e = time.Parse(http.TimeFormat, updates.BuildDate)
		fatalIf(probe.NewError(e), "Unable to parse BuildDate.", nil)
	}

	if latest.IsZero() {
		fatalIf(probe.NewError(errors.New("")),
			"Unable to validate any update available at this time. Please open an issue at https://github.com/minio/minio/issues", nil)
	}

	updateURLParse, err := url.Parse(updateURL)
	if err != nil {
		fatalIf(probe.NewError(err), "Unable to parse URL: "+updateURL, nil)
	}
	downloadURL := updateURLParse.Scheme + "://" +
		updateURLParse.Host + "/" + updates.Platforms[runtime.GOOS+"-"+runtime.GOARCH]

	updateMsg := updateMessage{
		Download: downloadURL,
		Version:  minioVersion,
	}
	if latest.After(current) {
		updateMsg.Update = true
	}

	if globalJSONFlag {
		Println(updateMsg.JSON())
	} else {
		Println(updateMsg)
	}
}

// main entry point for update command.
func mainUpdate(ctx *cli.Context) {
	// Check for update.
	if ctx.Bool("experimental") {
		getReleaseUpdate(minioUpdateExperimentalURL)
	} else {
		getReleaseUpdate(minioUpdateStableURL)
	}
}
