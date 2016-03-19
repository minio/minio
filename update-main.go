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
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/minio/pkg/probe"
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
	minioUpdateStableURL       = "https://dl.minio.io/server/minio/release/"
	minioUpdateExperimentalURL = "https://dl.minio.io/server/minio/experimental/"
)

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

func parseReleaseData(data string) (time.Time, *probe.Error) {
	releaseStr := strings.Fields(data)
	if len(releaseStr) < 2 {
		return time.Time{}, probe.NewError(errors.New("Update data malformed"))
	}
	releaseDate := releaseStr[1]
	releaseDateSplits := strings.SplitN(releaseDate, ".", 3)
	if len(releaseDateSplits) < 3 {
		return time.Time{}, probe.NewError(errors.New("Update data malformed"))
	}
	if releaseDateSplits[0] != "minio" {
		return time.Time{}, probe.NewError(errors.New("Update data malformed, missing minio tag"))
	}
	// "OFFICIAL" tag is still kept for backward compatibility, we should remove this for the next release.
	if releaseDateSplits[1] != "RELEASE" && releaseDateSplits[1] != "OFFICIAL" {
		return time.Time{}, probe.NewError(errors.New("Update data malformed, missing RELEASE tag"))
	}
	dateSplits := strings.SplitN(releaseDateSplits[2], "T", 2)
	if len(dateSplits) < 2 {
		return time.Time{}, probe.NewError(errors.New("Update data malformed, not in modified RFC3359 form"))
	}
	dateSplits[1] = strings.Replace(dateSplits[1], "-", ":", -1)
	date := strings.Join(dateSplits, "T")

	parsedDate, e := time.Parse(time.RFC3339, date)
	if e != nil {
		return time.Time{}, probe.NewError(e)
	}
	return parsedDate, nil
}

// verify updates for releases.
func getReleaseUpdate(updateURL string) {
	newUpdateURLPrefix := updateURL + "/" + runtime.GOOS + "-" + runtime.GOARCH
	newUpdateURL := newUpdateURLPrefix + "/minio.shasum"
	data, e := http.Get(newUpdateURL)
	fatalIf(probe.NewError(e), "Unable to read from update URL ‘"+newUpdateURL+"’.", nil)

	if minioVersion == "DEVELOPMENT.GOGET" {
		fatalIf(probe.NewError(errors.New("")),
			"Update mechanism is not supported for ‘go get’ based binary builds.  Please download official releases from https://minio.io/#minio", nil)
	}

	current, e := time.Parse(time.RFC3339, minioVersion)
	fatalIf(probe.NewError(e), "Unable to parse version string as time.", nil)

	if current.IsZero() {
		fatalIf(probe.NewError(errors.New("")),
			"Updates not supported for custom builds. Version field is empty. Please download official releases from https://minio.io/#minio", nil)
	}

	body, e := ioutil.ReadAll(data.Body)
	fatalIf(probe.NewError(e), "Fetching updates failed. Please try again.", nil)

	latest, err := parseReleaseData(string(body))
	fatalIf(err.Trace(updateURL), "Please report this issue at https://github.com/minio/minio/issues.", nil)

	if latest.IsZero() {
		fatalIf(probe.NewError(errors.New("")),
			"Unable to validate any update available at this time. Please open an issue at https://github.com/minio/minio/issues", nil)
	}

	var downloadURL string
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		downloadURL = newUpdateURLPrefix + "/minio.zip"
	} else {
		downloadURL = newUpdateURLPrefix + "/minio.gz"
	}

	updateMsg := updateMessage{
		Download: downloadURL,
		Version:  minioVersion,
	}
	if latest.After(current) {
		updateMsg.Update = true
	}
	console.Println(updateMsg)
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
