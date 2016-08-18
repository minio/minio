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

package cmd

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
	minioUpdateStableURL       = "https://dl.minio.io/server/minio/release"
	minioUpdateExperimentalURL = "https://dl.minio.io/server/minio/experimental"
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
	msg := colorizeUpdateMessage(u.Download)
	return msg
}

// JSON jsonified update message.
func (u updateMessage) JSON() string {
	u.Status = "success"
	updateMessageJSONBytes, err := json.Marshal(u)
	fatalIf((err), "Unable to marshal into JSON.")

	return string(updateMessageJSONBytes)
}

func parseReleaseData(data string) (time.Time, error) {
	releaseStr := strings.Fields(data)
	if len(releaseStr) < 2 {
		return time.Time{}, errors.New("Update data malformed")
	}
	releaseDate := releaseStr[1]
	releaseDateSplits := strings.SplitN(releaseDate, ".", 3)
	if len(releaseDateSplits) < 3 {
		return time.Time{}, (errors.New("Update data malformed"))
	}
	if releaseDateSplits[0] != "minio" {
		return time.Time{}, (errors.New("Update data malformed, missing minio tag"))
	}
	// "OFFICIAL" tag is still kept for backward compatibility, we should remove this for the next release.
	if releaseDateSplits[1] != "RELEASE" && releaseDateSplits[1] != "OFFICIAL" {
		return time.Time{}, (errors.New("Update data malformed, missing RELEASE tag"))
	}
	dateSplits := strings.SplitN(releaseDateSplits[2], "T", 2)
	if len(dateSplits) < 2 {
		return time.Time{}, (errors.New("Update data malformed, not in modified RFC3359 form"))
	}
	dateSplits[1] = strings.Replace(dateSplits[1], "-", ":", -1)
	date := strings.Join(dateSplits, "T")

	parsedDate, err := time.Parse(time.RFC3339, date)
	if err != nil {
		return time.Time{}, err
	}
	return parsedDate, nil
}

// verify updates for releases.
func getReleaseUpdate(updateURL string, noError bool) updateMessage {
	// Construct a new update url.
	newUpdateURLPrefix := updateURL + "/" + runtime.GOOS + "-" + runtime.GOARCH
	newUpdateURL := newUpdateURLPrefix + "/minio.shasum"

	// Get the downloadURL.
	var downloadURL string
	switch runtime.GOOS {
	case "windows":
		// For windows.
		downloadURL = newUpdateURLPrefix + "/minio.exe?update=yes"
	default:
		// For all other operating systems.
		downloadURL = newUpdateURLPrefix + "/minio?update=yes"
	}

	// Initialize update message.
	updateMsg := updateMessage{
		Download: downloadURL,
		Version:  Version,
	}

	// Instantiate a new client with 3 sec timeout.
	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	// Fetch new update.
	data, err := client.Get(newUpdateURL)
	if err != nil && noError {
		return updateMsg
	}
	fatalIf((err), "Unable to read from update URL ‘"+newUpdateURL+"’.")

	// Error out if 'update' command is issued for development based builds.
	if Version == "DEVELOPMENT.GOGET" && !noError {
		fatalIf((errors.New("")),
			"Update mechanism is not supported for ‘go get’ based binary builds. Please download official releases from https://minio.io/#minio")
	}

	// Parse current minio version into RFC3339.
	current, err := time.Parse(time.RFC3339, Version)
	if err != nil && noError {
		return updateMsg
	}
	fatalIf((err), "Unable to parse version string as time.")

	// Verify if current minio version is zero.
	if current.IsZero() && !noError {
		fatalIf((errors.New("")),
			"Updates mechanism is not supported for custom builds. Please download official releases from https://minio.io/#minio")
	}

	// Verify if we have a valid http response i.e http.StatusOK.
	if data != nil {
		if data.StatusCode != http.StatusOK {
			// Return quickly if noError is set.
			if noError {
				return updateMsg
			}
			fatalIf((errors.New("")), "Failed to retrieve update notice. "+data.Status)
		}
	}

	// Read the response body.
	updateBody, err := ioutil.ReadAll(data.Body)
	if err != nil && noError {
		return updateMsg
	}
	fatalIf((err), "Failed to retrieve update notice. Please try again later.")

	// Parse the date if its valid.
	latest, err := parseReleaseData(string(updateBody))
	if err != nil && noError {
		return updateMsg
	}
	errMsg := "Failed to retrieve update notice. Please try again later. Please report this issue at https://github.com/minio/minio/issues"
	fatalIf(err, errMsg)

	// Verify if the date is not zero.
	if latest.IsZero() && !noError {
		fatalIf((errors.New("")), errMsg)
	}

	// Is the update latest?.
	if latest.After(current) {
		updateMsg.Update = true
	}

	// Return update message.
	return updateMsg
}

// main entry point for update command.
func mainUpdate(ctx *cli.Context) {
	// Print all errors as they occur.
	noError := false

	// Check for update.
	if ctx.Bool("experimental") {
		console.Println(getReleaseUpdate(minioUpdateExperimentalURL, noError))
	} else {
		console.Println(getReleaseUpdate(minioUpdateStableURL, noError))
	}
}
