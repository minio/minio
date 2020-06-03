/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017 MinIO, Inc.
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
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/inconshreveable/go-update"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
	_ "github.com/minio/sha256-simd" // Needed for sha256 hash verifier.
)

const (
	minioReleaseTagTimeLayout = "2006-01-02T15-04-05Z"
	minioOSARCH               = runtime.GOOS + "-" + runtime.GOARCH
	minioReleaseURL           = "https://dl.min.io/server/minio/release/" + minioOSARCH + SlashSeparator
)

var (
	// Newer official download info URLs appear earlier below.
	minioReleaseInfoURLs = []string{
		minioReleaseURL + "minio.sha256sum",
		minioReleaseURL + "minio.shasum",
	}

	// For windows our files have .exe additionally.
	minioReleaseWindowsInfoURLs = []string{
		minioReleaseURL + "minio.exe.sha256sum",
		minioReleaseURL + "minio.exe.shasum",
	}
)

// minioVersionToReleaseTime - parses a standard official release
// MinIO version string.
//
// An official binary's version string is the release time formatted
// with RFC3339 (in UTC) - e.g. `2017-09-29T19:16:56Z`
func minioVersionToReleaseTime(version string) (releaseTime time.Time, err error) {
	return time.Parse(time.RFC3339, version)
}

// releaseTimeToReleaseTag - converts a time to a string formatted as
// an official MinIO release tag.
//
// An official minio release tag looks like:
// `RELEASE.2017-09-29T19-16-56Z`
func releaseTimeToReleaseTag(releaseTime time.Time) string {
	return "RELEASE." + releaseTime.Format(minioReleaseTagTimeLayout)
}

// releaseTagToReleaseTime - reverse of `releaseTimeToReleaseTag()`
func releaseTagToReleaseTime(releaseTag string) (releaseTime time.Time, err error) {
	fields := strings.Split(releaseTag, ".")
	if len(fields) < 2 || len(fields) > 3 {
		return releaseTime, fmt.Errorf("%s is not a valid release tag", releaseTag)
	}
	if fields[0] != "RELEASE" {
		return releaseTime, fmt.Errorf("%s is not a valid release tag", releaseTag)
	}
	return time.Parse(minioReleaseTagTimeLayout, fields[1])
}

// getModTime - get the file modification time of `path`
func getModTime(path string) (t time.Time, err error) {
	// Convert to absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return t, fmt.Errorf("Unable to get absolute path of %s. %w", path, err)
	}

	// Version is minio non-standard, we will use minio binary's
	// ModTime as release time.
	fi, err := os.Stat(absPath)
	if err != nil {
		return t, fmt.Errorf("Unable to get ModTime of %s. %w", absPath, err)
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
	if env.Get("MINIO_CI_CD", "") == "" {
		_, err := os.Stat("/.dockerenv")
		if os.IsNotExist(err) {
			return false
		}

		// Log error, as we will not propagate it to caller
		logger.LogIf(GlobalContext, err)

		return err == nil
	}
	return false
}

// IsDCOS returns true if minio is running in DCOS.
func IsDCOS() bool {
	if env.Get("MINIO_CI_CD", "") == "" {
		// http://mesos.apache.org/documentation/latest/docker-containerizer/
		// Mesos docker containerizer sets this value
		return env.Get("MESOS_CONTAINER_NAME", "") != ""
	}
	return false
}

// IsKubernetesReplicaSet returns true if minio is running in kubernetes replica set.
func IsKubernetesReplicaSet() bool {
	return IsKubernetes() && (env.Get("KUBERNETES_REPLICA_SET", "") != "")
}

// IsKubernetes returns true if minio is running in kubernetes.
func IsKubernetes() bool {
	if env.Get("MINIO_CI_CD", "") == "" {
		// Kubernetes env used to validate if we are
		// indeed running inside a kubernetes pod
		// is KUBERNETES_SERVICE_HOST but in future
		// we might need to enhance this.
		return env.Get("KUBERNETES_SERVICE_HOST", "") != ""
	}
	return false
}

// IsBOSH returns true if minio is deployed from a bosh package
func IsBOSH() bool {
	// "/var/vcap/bosh" exists in BOSH deployed instance.
	_, err := os.Stat("/var/vcap/bosh")
	if os.IsNotExist(err) {
		return false
	}

	// Log error, as we will not propagate it to caller
	logger.LogIf(GlobalContext, err)

	return err == nil
}

// MinIO Helm chart uses DownwardAPIFile to write pod label info to /podinfo/labels
// More info: https://kubernetes.io/docs/tasks/inject-data-application/downward-api-volume-expose-pod-information/#store-pod-fields
// Check if this is Helm package installation and report helm chart version
func getHelmVersion(helmInfoFilePath string) string {
	// Read the file exists.
	helmInfoFile, err := os.Open(helmInfoFilePath)
	if err != nil {
		// Log errors and return "" as MinIO can be deployed
		// without Helm charts as well.
		if !os.IsNotExist(err) {
			reqInfo := (&logger.ReqInfo{}).AppendTags("helmInfoFilePath", helmInfoFilePath)
			ctx := logger.SetReqInfo(GlobalContext, reqInfo)
			logger.LogIf(ctx, err)
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
//   MinIO (<OS>; <ARCH>[; <MODE>][; dcos][; kubernetes][; docker][; source]) MinIO/<VERSION> MinIO/<RELEASE-TAG> MinIO/<COMMIT-ID> [MinIO/universe-<PACKAGE-NAME>] [MinIO/helm-<HELM-VERSION>]
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

	uaAppend("MinIO (", runtime.GOOS)
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

	uaAppend(") MinIO/", Version)
	uaAppend(" MinIO/", ReleaseTag)
	uaAppend(" MinIO/", CommitID)
	if IsDCOS() {
		universePkgVersion := env.Get("MARATHON_APP_LABEL_DCOS_PACKAGE_VERSION", "")
		// On DC/OS environment try to the get universe package version.
		if universePkgVersion != "" {
			uaAppend(" MinIO/universe-", universePkgVersion)
		}
	}

	if IsKubernetes() {
		// In Kubernetes environment, try to fetch the helm package version
		helmChartVersion := getHelmVersion("/podinfo/labels")
		if helmChartVersion != "" {
			uaAppend(" MinIO/helm-", helmChartVersion)
		}
	}

	pcfTileVersion := env.Get("MINIO_PCF_TILE_VERSION", "")
	if pcfTileVersion != "" {
		uaAppend(" MinIO/pcf-tile-", pcfTileVersion)
	}

	return strings.Join(userAgentParts, "")
}

func downloadReleaseURL(releaseChecksumURL string, timeout time.Duration, mode string) (content string, err error) {
	req, err := http.NewRequest(http.MethodGet, releaseChecksumURL, nil)
	if err != nil {
		return content, AdminError{
			Code:       AdminUpdateUnexpectedFailure,
			Message:    err.Error(),
			StatusCode: http.StatusInternalServerError,
		}
	}
	req.Header.Set("User-Agent", getUserAgent(mode))

	client := &http.Client{Transport: getUpdateTransport(timeout)}
	resp, err := client.Do(req)
	if err != nil {
		if xnet.IsNetworkOrHostDown(err) {
			return content, AdminError{
				Code:       AdminUpdateURLNotReachable,
				Message:    err.Error(),
				StatusCode: http.StatusServiceUnavailable,
			}
		}
		return content, AdminError{
			Code:       AdminUpdateUnexpectedFailure,
			Message:    err.Error(),
			StatusCode: http.StatusInternalServerError,
		}
	}
	if resp == nil {
		return content, AdminError{
			Code:       AdminUpdateUnexpectedFailure,
			Message:    fmt.Sprintf("No response from server to download URL %s", releaseChecksumURL),
			StatusCode: http.StatusInternalServerError,
		}
	}
	defer xhttp.DrainBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return content, AdminError{
			Code:       AdminUpdateUnexpectedFailure,
			Message:    fmt.Sprintf("Error downloading URL %s. Response: %v", releaseChecksumURL, resp.Status),
			StatusCode: resp.StatusCode,
		}
	}
	contentBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return content, AdminError{
			Code:       AdminUpdateUnexpectedFailure,
			Message:    fmt.Sprintf("Error reading response. %s", err),
			StatusCode: http.StatusInternalServerError,
		}
	}

	return string(contentBytes), nil
}

// DownloadReleaseData - downloads release data from minio official server.
func DownloadReleaseData(timeout time.Duration, mode string) (data string, err error) {
	releaseURLs := minioReleaseInfoURLs
	if runtime.GOOS == globalWindowsOSName {
		releaseURLs = minioReleaseWindowsInfoURLs
	}

	return func() (data string, err error) {
		for _, url := range releaseURLs {
			data, err = downloadReleaseURL(url, timeout, mode)
			if err == nil {
				return data, nil
			}
		}
		return data, err
	}()
}

// parseReleaseData - parses release info file content fetched from
// official minio download server.
//
// The expected format is a single line with two words like:
//
// fbe246edbd382902db9a4035df7dce8cb441357d minio.RELEASE.2016-10-07T01-16-39Z.<hotfix_optional>
//
// The second word must be `minio.` appended to a standard release tag.
func parseReleaseData(data string) (sha256Hex string, releaseTime time.Time, err error) {
	defer func() {
		if err != nil {
			err = AdminError{
				Code:       AdminUpdateUnexpectedFailure,
				Message:    err.Error(),
				StatusCode: http.StatusInternalServerError,
			}
		}
	}()

	fields := strings.Fields(data)
	if len(fields) != 2 {
		err = fmt.Errorf("Unknown release data `%s`", data)
		return sha256Hex, releaseTime, err
	}

	sha256Hex = fields[0]
	releaseInfo := fields[1]

	// Split release of style minio.RELEASE.2019-08-21T19-40-07Z.<hotfix>
	nfields := strings.SplitN(releaseInfo, ".", 2)
	if len(nfields) != 2 {
		err = fmt.Errorf("Unknown release information `%s`", releaseInfo)
		return sha256Hex, releaseTime, err
	}
	if nfields[0] != "minio" {
		err = fmt.Errorf("Unknown release `%s`", releaseInfo)
		return sha256Hex, releaseTime, err
	}

	releaseTime, err = releaseTagToReleaseTime(nfields[1])
	if err != nil {
		err = fmt.Errorf("Unknown release tag format. %w", err)
	}

	return sha256Hex, releaseTime, err
}

const updateTimeout = 10 * time.Second

func getUpdateTransport(timeout time.Duration) http.RoundTripper {
	var updateTransport http.RoundTripper = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           xhttp.NewCustomDialContext(timeout, timeout),
		IdleConnTimeout:       timeout,
		TLSHandshakeTimeout:   timeout,
		ExpectContinueTimeout: timeout,
		TLSClientConfig: &tls.Config{
			RootCAs: globalRootCAs,
		},
		DisableCompression: true,
	}
	return updateTransport
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
	kubernetesDeploymentDoc = "https://docs.min.io/docs/deploy-minio-on-kubernetes"

	// Mesos deployment doc link.
	mesosDeploymentDoc = "https://docs.min.io/docs/deploy-minio-on-dc-os"
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

func doUpdate(updateURL, sha256Hex, mode string) (err error) {
	var sha256Sum []byte
	sha256Sum, err = hex.DecodeString(sha256Hex)
	if err != nil {
		return AdminError{
			Code:       AdminUpdateUnexpectedFailure,
			Message:    err.Error(),
			StatusCode: http.StatusInternalServerError,
		}
	}

	clnt := &http.Client{
		Transport: getUpdateTransport(30 * time.Second),
	}
	req, err := http.NewRequest(http.MethodGet, updateURL, nil)
	if err != nil {
		return AdminError{
			Code:       AdminUpdateUnexpectedFailure,
			Message:    err.Error(),
			StatusCode: http.StatusInternalServerError,
		}
	}

	req.Header.Set("User-Agent", getUserAgent(mode))

	resp, err := clnt.Do(req)
	if err != nil {
		if xnet.IsNetworkOrHostDown(err) {
			return AdminError{
				Code:       AdminUpdateURLNotReachable,
				Message:    err.Error(),
				StatusCode: http.StatusServiceUnavailable,
			}
		}
		return AdminError{
			Code:       AdminUpdateUnexpectedFailure,
			Message:    err.Error(),
			StatusCode: http.StatusInternalServerError,
		}
	}
	defer xhttp.DrainBody(resp.Body)

	// FIXME: add support for gpg verification as well.
	if err = update.Apply(resp.Body,
		update.Options{
			Hash:     crypto.SHA256,
			Checksum: sha256Sum,
		},
	); err != nil {
		if rerr := update.RollbackError(err); rerr != nil {
			return AdminError{
				Code:       AdminUpdateApplyFailure,
				Message:    fmt.Sprintf("Failed to rollback from bad update: %v", rerr),
				StatusCode: http.StatusInternalServerError,
			}
		}
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return AdminError{
				Code: AdminUpdateApplyFailure,
				Message: fmt.Sprintf("Unable to update the binary at %s: %v",
					filepath.Dir(pathErr.Path), pathErr.Err),
				StatusCode: http.StatusForbidden,
			}
		}
		return AdminError{
			Code:       AdminUpdateApplyFailure,
			Message:    err.Error(),
			StatusCode: http.StatusInternalServerError,
		}
	}

	return nil
}
