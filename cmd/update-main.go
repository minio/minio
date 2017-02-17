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
	"os/exec"
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

func getCurrentReleaseTime(minioVersion, minioBinaryPath string) (releaseTime time.Time, err error) {
	if releaseTime, err = time.Parse(time.RFC3339, minioVersion); err == nil {
		return releaseTime, err
	}

	if !filepath.IsAbs(minioBinaryPath) {
		// Make sure to look for the absolute path of the binary.
		minioBinaryPath, err = exec.LookPath(minioBinaryPath)
		if err != nil {
			return releaseTime, err
		}
	}

	// Looks like version is minio non-standard, we use minio binary's ModTime as release time.
	fi, err := osStat(minioBinaryPath)
	if err != nil {
		err = fmt.Errorf("Unable to get ModTime of %s. %s", minioBinaryPath, err)
	} else {
		releaseTime = fi.ModTime().UTC()
	}

	return releaseTime, err
}

// GetCurrentReleaseTime - returns this process's release time.  If it is official minio version,
// parsed version is returned else minio binary's mod time is returned.
func GetCurrentReleaseTime() (releaseTime time.Time, err error) {
	return getCurrentReleaseTime(Version, os.Args[0])
}

// Check if we are indeed inside docker.
// https://github.com/moby/moby/blob/master/daemon/initlayer/setup_unix.go#L25
//
//     "/.dockerenv":      "file",
//
func isDocker(dockerEnvFile string) (ok bool, err error) {
	_, err = os.Stat(dockerEnvFile)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return false, err
	}
	return true, nil
}

// IsDocker - returns if the environment minio is running
// is docker or not.
func IsDocker() bool {
	found, err := isDocker("/.dockerenv")
	// We don't need to fail for this check, log
	// an error and return false.
	errorIf(err, "Error in docker check.")

	return found
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
	// Log errors and return "" as Minio can be deployed without Helm charts as well.
	if err != nil && !os.IsNotExist(err) {
		errorIf(err, "Unable to read %s", helmInfoFilePath)
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

func isSourceBuild(minioVersion string) bool {
	_, err := time.Parse(time.RFC3339, minioVersion)
	return err != nil
}

// IsSourceBuild - returns if this binary is made from source or not.
func IsSourceBuild() bool {
	return isSourceBuild(Version)
}

// DO NOT CHANGE USER AGENT STYLE.
// The style should be
//
//   Minio (<OS>; <ARCH>[; dcos][; kubernetes][; docker][; source]) Minio/<VERSION> Minio/<RELEASE-TAG> Minio/<COMMIT-ID> [Minio/univers-<PACKAGE_NAME>]
//
// For any change here should be discussed by openning an issue at https://github.com/minio/minio/issues.
func getUserAgent(mode string) string {
	userAgent := "Minio (" + runtime.GOOS + "; " + runtime.GOARCH
	if mode != "" {
		userAgent += "; " + mode
	}
	if IsDCOS() {
		userAgent += "; dcos"
	}
	if IsKubernetes() {
		userAgent += "; kubernetes"
	}
	if IsDocker() {
		userAgent += "; docker"
	}
	if IsSourceBuild() {
		userAgent += "; source"
	}

	userAgent += ") Minio/" + Version + " Minio/" + ReleaseTag + " Minio/" + CommitID
	if IsDCOS() {
		universePkgVersion := os.Getenv("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION")
		// On DC/OS environment try to the get universe package version.
		if universePkgVersion != "" {
			userAgent += " Minio/" + "universe-" + universePkgVersion
		}
	}

	if IsKubernetes() {
		// In Kubernetes environment, try to fetch the helm package version
		helmChartVersion := getHelmVersion("/podinfo/labels")
		if helmChartVersion != "" {
			userAgent += " Minio/" + "helm-" + helmChartVersion
		}
	}

	return userAgent
}

func downloadReleaseData(releaseChecksumURL string, timeout time.Duration, mode string) (data string, err error) {
	req, err := http.NewRequest("GET", releaseChecksumURL, nil)
	if err != nil {
		return data, err
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
		return data, err
	}
	if resp == nil {
		return data, fmt.Errorf("No response from server to download URL %s", releaseChecksumURL)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return data, fmt.Errorf("Error downloading URL %s. Response: %v", releaseChecksumURL, resp.Status)
	}
	dataBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return data, fmt.Errorf("Error reading response. %s", err)
	}

	data = string(dataBytes)
	return data, err
}

// DownloadReleaseData - downloads release data from minio official server.
func DownloadReleaseData(timeout time.Duration, mode string) (data string, err error) {
	data, err = downloadReleaseData(minioReleaseURL+"minio.shasum", timeout, mode)
	if err == nil {
		return data, nil
	}
	return downloadReleaseData(minioReleaseURL+"minio.sha256sum", timeout, mode)
}

func parseReleaseData(data string) (releaseTime time.Time, err error) {
	fields := strings.Fields(data)
	if len(fields) != 2 {
		err = fmt.Errorf("Unknown release data `%s`", data)
		return releaseTime, err
	}

	releaseInfo := fields[1]
	if fields = strings.Split(releaseInfo, "."); len(fields) != 3 {
		err = fmt.Errorf("Unknown release information `%s`", releaseInfo)
		return releaseTime, err
	}

	if !(fields[0] == "minio" && fields[1] == "RELEASE") {
		err = fmt.Errorf("Unknown release '%s'", releaseInfo)
		return releaseTime, err
	}

	releaseTime, err = time.Parse(minioReleaseTagTimeLayout, fields[2])
	if err != nil {
		err = fmt.Errorf("Unknown release time format. %s", err)
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

func getDownloadURL(buildDate time.Time) (downloadURL string) {
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
		rTag := "RELEASE." + buildDate.Format(minioReleaseTagTimeLayout)
		return fmt.Sprintf("docker pull minio/minio:%s", rTag)
	}

	// For binary only installations, then we just show binary download link.
	if runtime.GOOS == globalWindowsOSName {
		return minioReleaseURL + "minio.exe"
	}

	return minioReleaseURL + "minio"
}

func getUpdateInfo(timeout time.Duration, mode string) (older time.Duration,
	downloadURL string, err error) {

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
		downloadURL = getDownloadURL(latestReleaseTime)
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
