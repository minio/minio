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
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
)

// Check for new software updates.
var updateCmd = cli.Command{
	Name:   "update",
	Usage:  "Check for a new software update.",
	Action: mainUpdate,
	Flags:  globalFlags,
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
`,
}

// update URL endpoints.
const (
	minioUpdateStableURL = "https://dl.minio.io/server/minio/release"
)

// updateMessage container to hold update messages.
type updateMessage struct {
	Update    bool          `json:"update"`
	Download  string        `json:"downloadURL"`
	NewerThan time.Duration `json:"newerThan"`
}

// String colorized update message.
func (u updateMessage) String() string {
	if !u.Update {
		updateMessage := color.New(color.FgGreen, color.Bold).SprintfFunc()
		return updateMessage("You are already running the most recent version of ‘minio’.")
	}
	msg := colorizeUpdateMessage(u.Download, u.NewerThan)
	return msg
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
	// "OFFICIAL" tag is still kept for backward compatibility.
	// We should remove this for the next release.
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

// User Agent should always following the below style.
// Please open an issue to discuss any new changes here.
//
//       Minio (OS; ARCH) APP/VER APP/VER
var (
	userAgentSuffix = "Minio/" + Version + " " + "Minio/" + ReleaseTag + " " + "Minio/" + CommitID
)

// Check if the operating system is a docker container.
func isDocker() bool {
	cgroup, err := ioutil.ReadFile("/proc/self/cgroup")
	if err != nil && !os.IsNotExist(err) {
		errorIf(err, "Unable to read `cgroup` file.")
	}

	return bytes.Contains(cgroup, []byte("docker"))
}

// Check if the minio server binary was built with source.
func isSourceBuild() bool {
	return Version == "DEVELOPMENT.GOGET"
}

// Fetch the current version of the Minio server binary.
func getCurrentMinioVersion() (current time.Time, err error) {
	// For development builds we check for binary modTime
	// to validate against latest minio server release.
	if Version != "DEVELOPMENT.GOGET" {
		// Parse current minio version into RFC3339.
		current, err = time.Parse(time.RFC3339, Version)
		if err != nil {
			return time.Time{}, err
		}
		return current, nil
	} // else {
	// For all development builds through `go get`.
	// fall back to looking for version of the build
	// date of the binary itself.
	var fi os.FileInfo
	fi, err = os.Stat(os.Args[0])
	if err != nil {
		return time.Time{}, err
	}
	return fi.ModTime(), nil
}

// verify updates for releases.
func getReleaseUpdate(updateURL string, duration time.Duration) (updateMsg updateMessage, errMsg string, err error) {
	// Construct a new update url.
	newUpdateURLPrefix := updateURL + "/" + runtime.GOOS + "-" + runtime.GOARCH
	newUpdateURL := newUpdateURLPrefix + "/minio.shasum"

	// Get the downloadURL.
	var downloadURL string
	if isDocker() {
		downloadURL = "docker pull minio/minio"
	} else {
		switch runtime.GOOS {
		case "windows":
			// For windows.
			downloadURL = newUpdateURLPrefix + "/minio.exe"
		default:
			// For all other operating systems.
			downloadURL = newUpdateURLPrefix + "/minio"
		}
	}

	// Initialize update message.
	updateMsg = updateMessage{
		Download: downloadURL,
	}

	// Instantiate a new client with 3 sec timeout.
	client := &http.Client{
		Timeout: duration,
	}

	current, err := getCurrentMinioVersion()
	if err != nil {
		errMsg = "Unable to fetch the current version of Minio server."
		return
	}

	// Initialize new request.
	req, err := http.NewRequest("GET", newUpdateURL, nil)
	if err != nil {
		return
	}

	userAgentPrefix := func() string {
		prefix := "Minio (" + runtime.GOOS + "; " + runtime.GOARCH
		// if its a source build.
		if isSourceBuild() {
			if isDocker() {
				prefix = prefix + "; " + "docker; source) "
			} else {
				prefix = prefix + "; " + "source) "
			}
			return prefix
		} // else { not source.
		if isDocker() {
			prefix = prefix + "; " + "docker) "
		} else {
			prefix = prefix + ") "
		}
		return prefix
	}()

	// Set user agent.
	req.Header.Set("User-Agent", userAgentPrefix+" "+userAgentSuffix)

	// Fetch new update.
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	// Verify if we have a valid http response i.e http.StatusOK.
	if resp != nil {
		if resp.StatusCode != http.StatusOK {
			errMsg = "Failed to retrieve update notice."
			err = errors.New("http status : " + resp.Status)
			return
		}
	}

	// Read the response body.
	updateBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errMsg = "Failed to retrieve update notice. Please try again later."
		return
	}

	errMsg = "Failed to retrieve update notice. Please try again later. Please report this issue at https://github.com/minio/minio/issues"

	// Parse the date if its valid.
	latest, err := parseReleaseData(string(updateBody))
	if err != nil {
		return
	}

	// Verify if the date is not zero.
	if latest.IsZero() {
		err = errors.New("Release date cannot be zero. Please report this issue at https://github.com/minio/minio/issues")
		return
	}

	// Is the update latest?.
	if latest.After(current) {
		updateMsg.Update = true
		updateMsg.NewerThan = latest.Sub(current)
	}

	// Return update message.
	return updateMsg, "", nil
}

// main entry point for update command.
func mainUpdate(ctx *cli.Context) {

	// Set global variables after parsing passed arguments
	setGlobalsFromContext(ctx)

	// Initialization routine, such as config loading, enable logging, ..
	minioInit()

	if globalQuiet {
		return
	}

	// Check for update.
	var updateMsg updateMessage
	var errMsg string
	var err error
	var secs = time.Second * 3
	updateMsg, errMsg, err = getReleaseUpdate(minioUpdateStableURL, secs)
	fatalIf(err, errMsg)
	console.Println(updateMsg)
}
