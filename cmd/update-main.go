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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
)

// Check for new software updates.
var updateCmd = cli.Command{
	Name:   "update",
	Usage:  "Check for a new software update.",
	Action: mainUpdate,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "quiet",
			Usage: "Disable any update messages.",
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
   1 - New update is available.
  -1 - Error in getting update information.

EXAMPLES:
   1. Check if there is a new update available:
       $ {{.HelpName}}
`,
}

const (
	minioReleaseTagTimeLayout = "2006-01-02T15-04-05Z"
	minioReleaseURL           = "https://dl.minio.io/server/minio/release/" + runtime.GOOS + "-" + runtime.GOARCH + "/"
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
func parseReleaseData(data string) (releaseTime time.Time, err error) {
	fields := strings.Fields(data)
	if len(fields) != 2 {
		err = fmt.Errorf("Unknown release data `%s`", data)
		return releaseTime, err
	}

	releaseInfo := fields[1]

	fields = strings.SplitN(releaseInfo, ".", 2)
	if len(fields) != 2 {
		err = fmt.Errorf("Unknown release information `%s`", releaseInfo)
		return releaseTime, err
	}
	if fields[0] != "minio" {
		err = fmt.Errorf("Unknown release `%s`", releaseInfo)
		return releaseTime, err
	}

	releaseTime, err = releaseTagToReleaseTime(fields[1])
	if err != nil {
		err = fmt.Errorf("Unknown release tag format. %s", err)
	}

	return releaseTime, err
}

func getLatestReleaseTime(timeout time.Duration, mode string) (releaseTime time.Time, err error) {
	data, err := DownloadReleaseData(timeout, mode)
	if err != nil {
		return releaseTime, err
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

func getUpdateInfo(timeout time.Duration, mode string) (older time.Duration, downloadURL string, err error) {
	var currentReleaseTime, latestReleaseTime time.Time
	currentReleaseTime, err = GetCurrentReleaseTime()
	if err != nil {
		return older, downloadURL, err
	}

	latestReleaseTime, err = getLatestReleaseTime(timeout, mode)
	if err != nil {
		return older, downloadURL, err
	}

	if latestReleaseTime.After(currentReleaseTime) {
		older = latestReleaseTime.Sub(currentReleaseTime)
		downloadURL = getDownloadURL(releaseTimeToReleaseTag(latestReleaseTime))
	}

	return older, downloadURL, nil
}

func mainUpdate(ctx *cli.Context) {
	if len(ctx.Args()) != 0 {
		cli.ShowCommandHelpAndExit(ctx, "update", -1)
	}

	quiet := ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	if quiet {
		log.EnableQuiet()
	}

	minioMode := ""
	older, downloadURL, err := getUpdateInfo(10*time.Second, minioMode)
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}

	if updateMsg := computeUpdateMessage(downloadURL, older); updateMsg != "" {
		log.Println(updateMsg)
		os.Exit(1)
	}

	colorSprintf := color.New(color.FgGreen, color.Bold).SprintfFunc()
	log.Println(colorSprintf("You are already running the most recent version of ‘minio’."))
	os.Exit(0)
}
