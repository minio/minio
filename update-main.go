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
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/minio-xl/pkg/probe"
)

// Check for new software updates.
var updateCmd = cli.Command{
	Name:   "update",
	Usage:  "Check for new software updates.",
	Action: mainUpdate,
	CustomHelpTemplate: `Name:
   minio {{.Name}} - {{.Usage}}

USAGE:
   minio {{.Name}} release
   minio {{.Name}} experimental

EXAMPLES:
   1. Check for new official releases
      $ minio {{.Name}} release

   2. Check for new experimental releases
      $ minio {{.Name}} experimental
`,
}

// updates container to hold updates json
type updates struct {
	BuildDate string
	Platforms map[string]string
}

// updateMessage container to hold update messages
type updateMessage struct {
	Update   bool   `json:"update"`
	Download string `json:"downloadURL"`
	Version  string `json:"version"`
}

// String colorized update message
func (u updateMessage) String() string {
	if u.Update {
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
	updateMessage := color.New(color.FgGreen, color.Bold).SprintfFunc()
	return updateMessage("You are already running the most recent version of ‘minio’.")
}

// JSON jsonified update message
func (u updateMessage) JSON() string {
	updateMessageJSONBytes, err := json.Marshal(u)
	fatalIf(probe.NewError(err), "Unable to marshal into JSON.", nil)

	return string(updateMessageJSONBytes)
}

func getExperimentalUpdate() {
	current, e := time.Parse(time.RFC3339, minioVersion)
	fatalIf(probe.NewError(e), "Unable to parse Version string as time.", nil)

	if current.IsZero() {
		fatalIf(probe.NewError(errors.New("")), "Experimental updates are not supported for custom build. Version field is empty. Please download official releases from https://dl.minio.io:9000", nil)
	}

	resp, err := http.Get(minioExperimentalURL)
	fatalIf(probe.NewError(err), "Unable to initalize experimental URL.", nil)

	var experimentals updates
	decoder := json.NewDecoder(resp.Body)
	defer resp.Body.Close()
	e = decoder.Decode(&experimentals)
	fatalIf(probe.NewError(e), "Unable to decode experimental update notification.", nil)

	latest, e := time.Parse(time.RFC3339, experimentals.BuildDate)
	fatalIf(probe.NewError(e), "Unable to parse BuildDate.", nil)

	if latest.IsZero() {
		fatalIf(probe.NewError(errors.New("")), "Unable to validate any experimental update available at this time. Please open an issue at https://github.com/minio/minio/issues", nil)
	}

	minioExperimentalURLParse, err := url.Parse(minioExperimentalURL)
	if err != nil {
		fatalIf(probe.NewError(err), "Unable to parse URL: "+minioExperimentalURL, nil)
	}
	downloadURL := minioExperimentalURLParse.Scheme + "://" +
		minioExperimentalURLParse.Host + "/" + experimentals.Platforms[runtime.GOOS+"-"+runtime.GOARCH]

	updateMessage := updateMessage{
		Download: downloadURL,
		Version:  minioVersion,
	}
	if latest.After(current) {
		updateMessage.Update = true
	}
	if globalJSONFlag {
		Println(updateMessage.JSON())
	} else {
		Println(updateMessage)
	}
}

func getReleaseUpdate() {
	current, e := time.Parse(time.RFC3339, minioVersion)
	fatalIf(probe.NewError(e), "Unable to parse Version string as time.", nil)

	if current.IsZero() {
		fatalIf(probe.NewError(errors.New("")), "Updates not supported for custom build. Version field is empty. Please download official releases from https://dl.minio.io:9000", nil)
	}

	resp, err := http.Get(minioUpdateURL)
	fatalIf(probe.NewError(err), "Unable to initalize experimental URL.", nil)

	var releases updates
	decoder := json.NewDecoder(resp.Body)
	e = decoder.Decode(&releases)
	fatalIf(probe.NewError(e), "Unable to decode update notification.", nil)

	latest, e := time.Parse(time.RFC3339, releases.BuildDate)
	fatalIf(probe.NewError(e), "Unable to parse BuildDate.", nil)

	if latest.IsZero() {
		fatalIf(probe.NewError(errors.New("")), "Unable to validate any update available at this time. Please open an issue at https://github.com/minio/minio/issues", nil)
	}

	minioUpdateURLParse, err := url.Parse(minioUpdateURL)
	if err != nil {
		fatalIf(probe.NewError(err), "Unable to parse URL: "+minioUpdateURL, nil)
	}
	downloadURL := minioUpdateURLParse.Scheme +
		"://" + minioUpdateURLParse.Host + "/" + releases.Platforms[runtime.GOOS+"-"+runtime.GOARCH]
	updateMessage := updateMessage{
		Download: downloadURL,
		Version:  minioVersion,
	}
	if latest.After(current) {
		updateMessage.Update = true
	}

	if globalJSONFlag {
		Println(updateMessage.JSON())
	} else {
		Println(updateMessage)
	}
}

const (
	minioUpdateURL       = "https://dl.minio.io:9000/updates/minio/updates.json"
	minioExperimentalURL = "https://dl.minio.io:9000/updates/minio/experimental.json"
)

func checkUpdateSyntax(ctx *cli.Context) {
	if ctx.Args().First() == "help" || !ctx.Args().Present() {
		cli.ShowCommandHelpAndExit(ctx, "update", 1) // last argument is exit code
	}
	arg := strings.TrimSpace(ctx.Args().First())
	if arg != "release" && arg != "experimental" {
		fatalIf(probe.NewError(errInvalidArgument), "Unrecognized argument provided.", nil)
	}
}

// mainUpdate -
func mainUpdate(ctx *cli.Context) {
	checkUpdateSyntax(ctx)
	arg := strings.TrimSpace(ctx.Args().First())
	switch arg {
	case "release":
		getReleaseUpdate()
	case "experimental":
		getExperimentalUpdate()
	}
}
