/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"bufio"
	"crypto"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/inconshreveable/go-update"
	"github.com/minio/cli"
	_ "github.com/minio/sha256-simd" // Needed for sha256 hash verifier.
	"github.com/segmentio/go-prompt"
)

// Check for new software updates.
var updateCmd = cli.Command{
	Name:   "update",
	Usage:  "Check for a new software update.",
	Action: mainUpdate,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "quiet",
			Usage: "Disable any update prompt message.",
		},
	},
	CustomHelpTemplate: `Name:
   {{.HelpName}} - {{.Usage}}

USAGE:
   {{.HelpName}}{{if .VisibleFlags}} [FLAGS]{{end}}
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXIT STATUS:
   0 - You are already running the most recent version.
   1 - New update was applied successfully.
  -1 - Error in getting update information.

EXAMPLES:
   1. Check and update minio:
       $ {{.HelpName}}
`,
}

const (
	minioReleaseTagTimeLayout = "2006-01-02T15-04-05Z"
	minioOSARCH               = runtime.GOOS + "-" + runtime.GOARCH
	minioReleaseURL           = "https://dl.minio.io/server/minio/release/" + minioOSARCH + "/"
)

var (
	// Newer official download info URLs appear earlier below.
	minioReleaseInfoURLs = []string{
		minioReleaseURL + "minio.sha256sum",
		minioReleaseURL + "minio.shasum",
	}
)

// minioVersionToReleaseTime - parses a standard official release
// Minio version string.
//
// An official binary's version string is the release time formatted
// with RFC3339 (in UTC) - e.g. `2017-09-29T19:16:56Z`
func minioVersionToReleaseTime(version string) (releaseTime time.Time, err error) {
	return time.Parse(time.RFC3339, version)
}

// releaseTimeToReleaseTag - converts a time to a string formatted as
// an official Minio release tag.
//
// An official minio release tag looks like:
// `RELEASE.2017-09-29T19-16-56Z`
func releaseTimeToReleaseTag(releaseTime time.Time) string {
	return "RELEASE." + releaseTime.Format(minioReleaseTagTimeLayout)
}

// releaseTagToReleaseTime - reverse of `releaseTimeToReleaseTag()`
func releaseTagToReleaseTime(releaseTag string) (releaseTime time.Time, err error) {
	tagTimePart := strings.TrimPrefix(releaseTag, "RELEASE.")
	if tagTimePart == releaseTag {
		return releaseTime, fmt.Errorf("%s is not a valid release tag", releaseTag)
	}
	return time.Parse(minioReleaseTagTimeLayout, tagTimePart)
}

// getModTime - get the file modification time of `path`
func getModTime(path string) (t time.Time, err error) {
	// Convert to absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return t, fmt.Errorf("Unable to get absolute path of %s. %s", path, err)
	}

	// Version is minio non-standard, we will use minio binary's
	// ModTime as release time.
	fi, err := os.Stat(absPath)
	if err != nil {
		return t, fmt.Errorf("Unable to get ModTime of %s. %s", absPath, err)
	}

	// Return the ModTime
	return fi.ModTime().UTC(), nil
}

// GetCurrentReleaseTime - returns this process's release time.  If it
// is official minio version, parsed version is returned else minio
// binary's mod time is returned.
func GetCurrentReleaseTime() (releaseTime time.Time, err error) {
	if releaseTime, err = minioVersionToReleaseTime(Version); err == nil {
		return releaseTime, err
	}

	// Looks like version is minio non-standard, we use minio
	// binary's ModTime as release time:
	return getModTime(os.Args[0])
}

// IsDocker - returns if the environment minio is running in docker or
// not. The check is a simple file existence check.
//
// https://github.com/moby/moby/blob/master/daemon/initlayer/setup_unix.go#L25
//
//     "/.dockerenv":      "file",
//
func IsDocker() bool {
	_, err := os.Stat("/.dockerenv")
	if os.IsNotExist(err) {
		return false
	}

	// Log error, as we will not propagate it to caller
	errorIf(err, "Error in docker check.")

	return err == nil
}

// IsDCOS returns true if minio is running in DCOS.
func IsDCOS() bool {
	// http://mesos.apache.org/documentation/latest/docker-containerizer/
	// Mesos docker containerizer sets this value
	return os.Getenv("MESOS_CONTAINER_NAME") != ""
}

// IsKubernetes returns true if minio is running in kubernetes.
func IsKubernetes() bool {
	// Kubernetes env used to validate if we are
	// indeed running inside a kubernetes pod
	// is KUBERNETES_SERVICE_HOST but in future
	// we might need to enhance this.
	return os.Getenv("KUBERNETES_SERVICE_HOST") != ""
}

// IsBOSH returns true if minio is deployed from a bosh package
func IsBOSH() bool {
	// "/var/vcap/bosh" exists in BOSH deployed instance.
	_, err := os.Stat("/var/vcap/bosh")
	if os.IsNotExist(err) {
		return false
	}

	// Log error, as we will not propagate it to caller
	errorIf(err, "Error in BOSH check.")

	return err == nil
}

// Minio Helm chart uses DownwardAPIFile to write pod label info to /podinfo/labels
// More info: https://kubernetes.io/docs/tasks/inject-data-application/downward-api-volume-expose-pod-information/#store-pod-fields
// Check if this is Helm package installation and report helm chart version
func getHelmVersion(helmInfoFilePath string) string {
	// Read the file exists.
	helmInfoFile, err := os.Open(helmInfoFilePath)
	if err != nil {
		// Log errors and return "" as Minio can be deployed
		// without Helm charts as well.
		if !os.IsNotExist(err) {
			errorIf(err, "Unable to read %s", helmInfoFilePath)
		}
		return ""
	}

	scanner := bufio.NewScanner(helmInfoFile)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "chart=") {
			helmChartVersion := strings.TrimPrefix(scanner.Text(), "chart=")
			// remove quotes from the chart version
			return strings.Trim(helmChartVersion, `"`)
		}
	}

	return ""
}

// IsSourceBuild - returns if this binary is a non-official build from
// source code.
func IsSourceBuild() bool {
	_, err := minioVersionToReleaseTime(Version)
	return err != nil
}

// DO NOT CHANGE USER AGENT STYLE.
// The style should be
//
//   Minio (<OS>; <ARCH>[; <MODE>][; dcos][; kubernetes][; docker][; source]) Minio/<VERSION> Minio/<RELEASE-TAG> Minio/<COMMIT-ID> [Minio/universe-<PACKAGE-NAME>] [Minio/helm-<HELM-VERSION>]
//
// Any change here should be discussed by opening an issue at
// https://github.com/minio/minio/issues.
func getUserAgent(mode string) string {

	userAgentParts := []string{}
	// Helper function to concisely append a pair of strings to a
	// the user-agent slice.
	uaAppend := func(p, q string) {
		userAgentParts = append(userAgentParts, p, q)
	}

	uaAppend("Minio (", runtime.GOOS)
	uaAppend("; ", runtime.GOARCH)
	if mode != "" {
		uaAppend("; ", mode)
	}
	if IsDCOS() {
		uaAppend("; ", "dcos")
	}
	if IsKubernetes() {
		uaAppend("; ", "kubernetes")
	}
	if IsDocker() {
		uaAppend("; ", "docker")
	}
	if IsBOSH() {
		uaAppend("; ", "bosh")
	}
	if IsSourceBuild() {
		uaAppend("; ", "source")
	}

	uaAppend(") Minio/", Version)
	uaAppend(" Minio/", ReleaseTag)
	uaAppend(" Minio/", CommitID)
	if IsDCOS() {
		universePkgVersion := os.Getenv("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION")
		// On DC/OS environment try to the get universe package version.
		if universePkgVersion != "" {
			uaAppend(" Minio/universe-", universePkgVersion)
		}
	}

	if IsKubernetes() {
		// In Kubernetes environment, try to fetch the helm package version
		helmChartVersion := getHelmVersion("/podinfo/labels")
		if helmChartVersion != "" {
			uaAppend(" Minio/helm-", helmChartVersion)
		}
	}

	pcfTileVersion := os.Getenv("MINIO_PCF_TILE_VERSION")
	if pcfTileVersion != "" {
		uaAppend(" Minio/pcf-tile-", pcfTileVersion)
	}

	return strings.Join(userAgentParts, "")
}

func downloadReleaseURL(releaseChecksumURL string, timeout time.Duration, mode string) (content string, err error) {
	req, err := http.NewRequest("GET", releaseChecksumURL, nil)
	if err != nil {
		return content, err
	}
	req.Header.Set("User-Agent", getUserAgent(mode))

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			// need to close connection after usage.
			DisableKeepAlives: true,
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return content, err
	}
	if resp == nil {
		return content, fmt.Errorf("No response from server to download URL %s", releaseChecksumURL)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return content, fmt.Errorf("Error downloading URL %s. Response: %v", releaseChecksumURL, resp.Status)
	}
	contentBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return content, fmt.Errorf("Error reading response. %s", err)
	}

	return string(contentBytes), err
}

// DownloadReleaseData - downloads release data from minio official server.
func DownloadReleaseData(timeout time.Duration, mode string) (data string, err error) {
	for _, url := range minioReleaseInfoURLs {
		data, err = downloadReleaseURL(url, timeout, mode)
		if err == nil {
			return data, err
		}
	}
	return data, fmt.Errorf("Failed to fetch release URL - last error: %s", err)
}

// parseReleaseData - parses release info file content fetched from
// official minio download server.
//
// The expected format is a single line with two words like:
//
// fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z
//
// The second word must be `minio.` appended to a standard release tag.
func parseReleaseData(data string) (sha256Hex string, releaseTime time.Time, err error) {
	fields := strings.Fields(data)
	if len(fields) != 2 {
		err = fmt.Errorf("Unknown release data `%s`", data)
		return sha256Hex, releaseTime, err
	}

	sha256Hex = fields[0]
	releaseInfo := fields[1]

	fields = strings.SplitN(releaseInfo, ".", 2)
	if len(fields) != 2 {
		err = fmt.Errorf("Unknown release information `%s`", releaseInfo)
		return sha256Hex, releaseTime, err
	}
	if fields[0] != "minio" {
		err = fmt.Errorf("Unknown release `%s`", releaseInfo)
		return sha256Hex, releaseTime, err
	}

	releaseTime, err = releaseTagToReleaseTime(fields[1])
	if err != nil {
		err = fmt.Errorf("Unknown release tag format. %s", err)
	}

	return sha256Hex, releaseTime, err
}

func getLatestReleaseTime(timeout time.Duration, mode string) (sha256Hex string, releaseTime time.Time, err error) {
	data, err := DownloadReleaseData(timeout, mode)
	if err != nil {
		return sha256Hex, releaseTime, err
	}

	return parseReleaseData(data)
}

const (
	// Kubernetes deployment doc link.
	kubernetesDeploymentDoc = "https://docs.minio.io/docs/deploy-minio-on-kubernetes"

	// Mesos deployment doc link.
	mesosDeploymentDoc = "https://docs.minio.io/docs/deploy-minio-on-dc-os"
)

func getDownloadURL(releaseTag string) (downloadURL string) {
	// Check if we are in DCOS environment, return
	// deployment guide for update procedures.
	if IsDCOS() {
		return mesosDeploymentDoc
	}

	// Check if we are in kubernetes environment, return
	// deployment guide for update procedures.
	if IsKubernetes() {
		return kubernetesDeploymentDoc
	}

	// Check if we are docker environment, return docker update command
	if IsDocker() {
		// Construct release tag name.
		return fmt.Sprintf("docker pull minio/minio:%s", releaseTag)
	}

	// For binary only installations, we return link to the latest binary.
	if runtime.GOOS == "windows" {
		return minioReleaseURL + "minio.exe"
	}

	return minioReleaseURL + "minio"
}

func getUpdateInfo(timeout time.Duration, mode string) (updateMsg string, sha256Hex string, currentReleaseTime, latestReleaseTime time.Time, err error) {
	currentReleaseTime, err = GetCurrentReleaseTime()
	if err != nil {
		return updateMsg, sha256Hex, currentReleaseTime, latestReleaseTime, err
	}

	sha256Hex, latestReleaseTime, err = getLatestReleaseTime(timeout, mode)
	if err != nil {
		return updateMsg, sha256Hex, currentReleaseTime, latestReleaseTime, err
	}

	var older time.Duration
	var downloadURL string
	if latestReleaseTime.After(currentReleaseTime) {
		older = latestReleaseTime.Sub(currentReleaseTime)
		downloadURL = getDownloadURL(releaseTimeToReleaseTag(latestReleaseTime))
	}

	return prepareUpdateMessage(downloadURL, older), sha256Hex, currentReleaseTime, latestReleaseTime, nil
}

func doUpdate(sha256Hex string, latestReleaseTime time.Time, ok bool) (successMsg string, err error) {
	if !ok {
		successMsg = greenColorSprintf("Minio update to version RELEASE.%s cancelled.",
			latestReleaseTime.Format(minioReleaseTagTimeLayout))
		return successMsg, nil
	}
	var sha256Sum []byte
	sha256Sum, err = hex.DecodeString(sha256Hex)
	if err != nil {
		return successMsg, err
	}

	resp, err := http.Get(getDownloadURL(releaseTimeToReleaseTag(latestReleaseTime)))
	if err != nil {
		return successMsg, err
	}
	defer resp.Body.Close()

	// FIXME: add support for gpg verification as well.
	if err = update.RollbackError(update.Apply(resp.Body,
		update.Options{
			Hash:     crypto.SHA256,
			Checksum: sha256Sum,
		},
	)); err != nil {
		return successMsg, err
	}

	return greenColorSprintf("Minio updated to version RELEASE.%s successfully.",
		latestReleaseTime.Format(minioReleaseTagTimeLayout)), nil
}

func shouldUpdate(quiet bool, sha256Hex string, latestReleaseTime time.Time) (ok bool) {
	ok = true
	if !quiet {
		ok = prompt.Confirm(greenColorSprintf("Update to RELEASE.%s [%s]", latestReleaseTime.Format(minioReleaseTagTimeLayout), "yes"))
	}
	return ok
}

var greenColorSprintf = color.New(color.FgGreen, color.Bold).SprintfFunc()

func mainUpdate(ctx *cli.Context) {
	if len(ctx.Args()) != 0 {
		cli.ShowCommandHelpAndExit(ctx, "update", -1)
	}

	handleCommonEnvVars()

	quiet := ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	if quiet {
		log.EnableQuiet()
	}

	minioMode := ""
	updateMsg, sha256Hex, _, latestReleaseTime, err := getUpdateInfo(10*time.Second, minioMode)
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}

	// Nothing to update running the latest release.
	if updateMsg == "" {
		log.Println(greenColorSprintf("You are already running the most recent version of ‘minio’."))
		os.Exit(0)
	}

	log.Println(updateMsg)
	// if the in-place update is disabled then we shouldn't ask the
	// user to update the binaries.
	if strings.Contains(updateMsg, minioReleaseURL) && !globalInplaceUpdateDisabled {
		var successMsg string
		successMsg, err = doUpdate(sha256Hex, latestReleaseTime, shouldUpdate(quiet, sha256Hex, latestReleaseTime))
		if err != nil {
			log.Println(err)
			os.Exit(-1)
		}
		log.Println(successMsg)
		os.Exit(1)
	}
}
