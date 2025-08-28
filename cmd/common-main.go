// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/x509"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	fcolor "github.com/fatih/color"
	"github.com/go-openapi/loads"
	"github.com/inconshreveable/mousetrap"
	dns2 "github.com/miekg/dns"
	"github.com/minio/cli"
	consoleapi "github.com/minio/console/api"
	"github.com/minio/console/api/operations"
	consoleoauth2 "github.com/minio/console/pkg/auth/idp/oauth2"
	consoleCerts "github.com/minio/console/pkg/certs"
	"github.com/minio/kms-go/kes"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/certs"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/pkg/v3/env"
	xnet "github.com/minio/pkg/v3/net"
	"golang.org/x/term"
)

// serverDebugLog will enable debug printing
var (
	serverDebugLog     = env.Get("_MINIO_SERVER_DEBUG", config.EnableOff) == config.EnableOn
	currentReleaseTime time.Time
	orchestrated       = IsKubernetes() || IsDocker()
)

func init() {
	if !term.IsTerminal(int(os.Stdout.Fd())) || !term.IsTerminal(int(os.Stderr.Fd())) {
		color.TurnOff()
	}
	if env.Get("NO_COLOR", "") != "" || env.Get("TERM", "") == "dumb" {
		color.TurnOff()
	}

	if runtime.GOOS == "windows" {
		if mousetrap.StartedByExplorer() {
			fmt.Printf("Don't double-click %s\n", os.Args[0])
			fmt.Println("You need to open cmd.exe/PowerShell and run it from the command line")
			fmt.Println("Refer to the docs here on how to run it as a Windows Service https://github.com/minio/minio-service/tree/master/windows")
			fmt.Println("Press the Enter Key to Exit")
			fmt.Scanln()
			os.Exit(1)
		}
	}

	logger.Init(GOPATH, GOROOT)
	logger.RegisterError(config.FmtError)

	t, _ := minioVersionToReleaseTime(Version)
	if !t.IsZero() {
		globalVersionUnix = uint64(t.Unix())
	}

	globalIsCICD = env.Get("MINIO_CI_CD", "") != "" || env.Get("CI", "") != ""

	console.SetColor("Debug", fcolor.New())

	gob.Register(StorageErr(""))
	gob.Register(madmin.TimeInfo{})
	gob.Register(madmin.XFSErrorConfigs{})
	gob.Register(map[string]string{})
	gob.Register(map[string]any{})

	// All minio-go and madmin-go API operations shall be performed only once,
	// another way to look at this is we are turning off retries.
	minio.MaxRetry = 1
	madmin.MaxRetry = 1

	currentReleaseTime, _ = GetCurrentReleaseTime()
}

const consolePrefix = "CONSOLE_"

func minioConfigToConsoleFeatures() {
	os.Setenv("CONSOLE_PBKDF_SALT", globalDeploymentID())
	os.Setenv("CONSOLE_PBKDF_PASSPHRASE", globalDeploymentID())
	if globalMinioEndpoint != "" {
		os.Setenv("CONSOLE_MINIO_SERVER", globalMinioEndpoint)
	} else {
		// Explicitly set 127.0.0.1 so Console will automatically bypass TLS verification to the local S3 API.
		// This will save users from providing a certificate with IP or FQDN SAN that points to the local host.
		os.Setenv("CONSOLE_MINIO_SERVER", fmt.Sprintf("%s://127.0.0.1:%s", getURLScheme(globalIsTLS), globalMinioPort))
	}
	if value := env.Get(config.EnvMinIOLogQueryURL, ""); value != "" {
		os.Setenv("CONSOLE_LOG_QUERY_URL", value)
		if value := env.Get(config.EnvMinIOLogQueryAuthToken, ""); value != "" {
			os.Setenv("CONSOLE_LOG_QUERY_AUTH_TOKEN", value)
		}
	}
	if value := env.Get(config.EnvBrowserRedirectURL, ""); value != "" {
		os.Setenv("CONSOLE_BROWSER_REDIRECT_URL", value)
	}
	if value := env.Get(config.EnvConsoleDebugLogLevel, ""); value != "" {
		os.Setenv("CONSOLE_DEBUG_LOGLEVEL", value)
	}
	// pass the console subpath configuration
	if globalBrowserRedirectURL != nil {
		subPath := path.Clean(pathJoin(strings.TrimSpace(globalBrowserRedirectURL.Path), SlashSeparator))
		if subPath != SlashSeparator {
			os.Setenv("CONSOLE_SUBPATH", subPath)
		}
	}
	// Enable if prometheus URL is set.
	if value := env.Get(config.EnvMinIOPrometheusURL, ""); value != "" {
		os.Setenv("CONSOLE_PROMETHEUS_URL", value)
		if value := env.Get(config.EnvMinIOPrometheusJobID, "minio-job"); value != "" {
			os.Setenv("CONSOLE_PROMETHEUS_JOB_ID", value)
			// Support additional labels for more granular filtering.
			if value := env.Get(config.EnvMinIOPrometheusExtraLabels, ""); value != "" {
				os.Setenv("CONSOLE_PROMETHEUS_EXTRA_LABELS", value)
			}
		}
		// Support Prometheus Auth Token
		if value := env.Get(config.EnvMinIOPrometheusAuthToken, ""); value != "" {
			os.Setenv("CONSOLE_PROMETHEUS_AUTH_TOKEN", value)
		}
	}
	// Enable if LDAP is enabled.
	if globalIAMSys.LDAPConfig.Enabled() {
		os.Setenv("CONSOLE_LDAP_ENABLED", config.EnableOn)
	}
	// Handle animation in welcome page
	if value := env.Get(config.EnvBrowserLoginAnimation, "on"); value != "" {
		os.Setenv("CONSOLE_ANIMATED_LOGIN", value)
	}

	// Pass on the session duration environment variable, else we will default to 12 hours
	if valueSts := env.Get(config.EnvMinioStsDuration, ""); valueSts != "" {
		os.Setenv("CONSOLE_STS_DURATION", valueSts)
	} else if valueSession := env.Get(config.EnvBrowserSessionDuration, ""); valueSession != "" {
		os.Setenv("CONSOLE_STS_DURATION", valueSession)
	}

	os.Setenv("CONSOLE_MINIO_SITE_NAME", globalSite.Name())
	os.Setenv("CONSOLE_MINIO_SITE_REGION", globalSite.Region())
	os.Setenv("CONSOLE_MINIO_REGION", globalSite.Region())

	os.Setenv("CONSOLE_CERT_PASSWD", env.Get("MINIO_CERT_PASSWD", ""))

	// This section sets Browser (console) stored config
	if valueSCP := globalBrowserConfig.GetCSPolicy(); valueSCP != "" {
		os.Setenv("CONSOLE_SECURE_CONTENT_SECURITY_POLICY", valueSCP)
	}

	if hstsSeconds := globalBrowserConfig.GetHSTSSeconds(); hstsSeconds > 0 {
		isubdom := globalBrowserConfig.IsHSTSIncludeSubdomains()
		isprel := globalBrowserConfig.IsHSTSPreload()
		os.Setenv("CONSOLE_SECURE_STS_SECONDS", strconv.Itoa(hstsSeconds))
		os.Setenv("CONSOLE_SECURE_STS_INCLUDE_SUB_DOMAINS", isubdom)
		os.Setenv("CONSOLE_SECURE_STS_PRELOAD", isprel)
	}

	if valueRefer := globalBrowserConfig.GetReferPolicy(); valueRefer != "" {
		os.Setenv("CONSOLE_SECURE_REFERRER_POLICY", valueRefer)
	}

	globalSubnetConfig.ApplyEnv()
}

func buildOpenIDConsoleConfig() consoleoauth2.OpenIDPCfg {
	pcfgs := globalIAMSys.OpenIDConfig.ProviderCfgs
	m := make(map[string]consoleoauth2.ProviderConfig, len(pcfgs))
	for name, cfg := range pcfgs {
		callback := getConsoleEndpoints()[0] + "/oauth_callback"
		if cfg.RedirectURI != "" {
			callback = cfg.RedirectURI
		}
		m[name] = consoleoauth2.ProviderConfig{
			URL:                     cfg.URL.String(),
			DisplayName:             cfg.DisplayName,
			ClientID:                cfg.ClientID,
			ClientSecret:            cfg.ClientSecret,
			HMACSalt:                globalDeploymentID(),
			HMACPassphrase:          cfg.ClientID,
			Scopes:                  strings.Join(cfg.DiscoveryDoc.ScopesSupported, ","),
			Userinfo:                cfg.ClaimUserinfo,
			RedirectCallbackDynamic: cfg.RedirectURIDynamic,
			RedirectCallback:        callback,
			EndSessionEndpoint:      cfg.DiscoveryDoc.EndSessionEndpoint,
			RoleArn:                 cfg.GetRoleArn(),
		}
	}
	return m
}

func initConsoleServer() (*consoleapi.Server, error) {
	// unset all console_ environment variables.
	for _, cenv := range env.List(consolePrefix) {
		os.Unsetenv(cenv)
	}

	// enable all console environment variables
	minioConfigToConsoleFeatures()

	// set certs dir to minio directory
	consoleCerts.GlobalCertsDir = &consoleCerts.ConfigDir{
		Path: globalCertsDir.Get(),
	}
	consoleCerts.GlobalCertsCADir = &consoleCerts.ConfigDir{
		Path: globalCertsCADir.Get(),
	}

	// set certs before other console initialization
	consoleapi.GlobalRootCAs, consoleapi.GlobalPublicCerts, consoleapi.GlobalTLSCertsManager = globalRootCAs, globalPublicCerts, globalTLSCerts

	swaggerSpec, err := loads.Embedded(consoleapi.SwaggerJSON, consoleapi.FlatSwaggerJSON)
	if err != nil {
		return nil, err
	}

	api := operations.NewConsoleAPI(swaggerSpec)

	if !serverDebugLog {
		// Disable console logging if server debug log is not enabled
		noLog := func(string, ...any) {}

		consoleapi.LogInfo = noLog
		consoleapi.LogError = noLog
		api.Logger = noLog
	}

	// Pass in console application config. This needs to happen before the
	// ConfigureAPI() call.
	consoleapi.GlobalMinIOConfig = consoleapi.MinIOConfig{
		OpenIDProviders: buildOpenIDConsoleConfig(),
	}

	server := consoleapi.NewServer(api)
	// register all APIs
	server.ConfigureAPI()

	consolePort, _ := strconv.Atoi(globalMinioConsolePort)

	server.Host = globalMinioConsoleHost
	server.Port = consolePort
	consoleapi.Port = globalMinioConsolePort
	consoleapi.Hostname = globalMinioConsoleHost

	if globalIsTLS {
		// If TLS certificates are provided enforce the HTTPS.
		server.EnabledListeners = []string{"https"}
		server.TLSPort = consolePort
		// Need to store tls-port, tls-host un config variables so secure.middleware can read from there
		consoleapi.TLSPort = globalMinioConsolePort
		consoleapi.Hostname = globalMinioConsoleHost
	}

	return server, nil
}

// Check for updates and print a notification message
func checkUpdate(mode string) {
	updateURL := minioReleaseInfoURL
	if runtime.GOOS == globalWindowsOSName {
		updateURL = minioReleaseWindowsInfoURL
	}

	u, err := url.Parse(updateURL)
	if err != nil {
		return
	}

	if currentReleaseTime.IsZero() {
		return
	}

	_, lrTime, err := getLatestReleaseTime(u, 2*time.Second, mode)
	if err != nil {
		return
	}

	var older time.Duration
	var downloadURL string
	if lrTime.After(currentReleaseTime) {
		older = lrTime.Sub(currentReleaseTime)
		downloadURL = getDownloadURL(releaseTimeToReleaseTag(lrTime))
	}

	updateMsg := prepareUpdateMessage(downloadURL, older)
	if updateMsg == "" {
		return
	}

	logger.Info(prepareUpdateMessage("Run `mc admin update ALIAS`", lrTime.Sub(currentReleaseTime)))
}

func newConfigDir(dir string, dirSet bool, getDefaultDir func() string) (*ConfigDir, error) {
	if dir == "" {
		dir = getDefaultDir()
	}

	if dir == "" {
		if !dirSet {
			return nil, fmt.Errorf("missing option must be provided")
		}
		return nil, fmt.Errorf("provided option cannot be empty")
	}

	// Disallow relative paths, figure out absolute paths.
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	err = mkdirAllIgnorePerm(dirAbs)
	if err != nil {
		return nil, fmt.Errorf("unable to create the directory `%s`: %w", dirAbs, err)
	}

	return &ConfigDir{path: dirAbs}, nil
}

func buildServerCtxt(ctx *cli.Context, ctxt *serverCtxt) (err error) {
	// Get "json" flag from command line argument and
	ctxt.JSON = ctx.IsSet("json") || ctx.GlobalIsSet("json")
	// Get quiet flag from command line argument.
	ctxt.Quiet = ctx.IsSet("quiet") || ctx.GlobalIsSet("quiet")
	// Get anonymous flag from command line argument.
	ctxt.Anonymous = ctx.IsSet("anonymous") || ctx.GlobalIsSet("anonymous")
	// Fetch address option
	ctxt.Addr = ctx.GlobalString("address")
	if ctxt.Addr == "" || ctxt.Addr == ":"+GlobalMinioDefaultPort {
		ctxt.Addr = ctx.String("address")
	}

	// Fetch console address option
	ctxt.ConsoleAddr = ctx.GlobalString("console-address")
	if ctxt.ConsoleAddr == "" {
		ctxt.ConsoleAddr = ctx.String("console-address")
	}

	if cxml := ctx.String("crossdomain-xml"); cxml != "" {
		buf, err := os.ReadFile(cxml)
		if err != nil {
			return err
		}
		ctxt.CrossDomainXML = string(buf)
	}

	// Check "no-compat" flag from command line argument.
	ctxt.StrictS3Compat = !ctx.IsSet("no-compat") && !ctx.GlobalIsSet("no-compat")

	switch {
	case ctx.IsSet("config-dir"):
		ctxt.ConfigDir = ctx.String("config-dir")
		ctxt.configDirSet = true
	case ctx.GlobalIsSet("config-dir"):
		ctxt.ConfigDir = ctx.GlobalString("config-dir")
		ctxt.configDirSet = true
	}

	switch {
	case ctx.IsSet("certs-dir"):
		ctxt.CertsDir = ctx.String("certs-dir")
		ctxt.certsDirSet = true
	case ctx.GlobalIsSet("certs-dir"):
		ctxt.CertsDir = ctx.GlobalString("certs-dir")
		ctxt.certsDirSet = true
	}

	memAvailable := availableMemory()
	if ctx.IsSet("memlimit") || ctx.GlobalIsSet("memlimit") {
		memlimit := ctx.String("memlimit")
		if memlimit == "" {
			memlimit = ctx.GlobalString("memlimit")
		}
		mlimit, err := humanize.ParseBytes(memlimit)
		if err != nil {
			return err
		}
		if mlimit > memAvailable {
			logger.Info("WARNING: maximum memory available (%s) smaller than specified --memlimit=%s, ignoring --memlimit value",
				humanize.IBytes(memAvailable), memlimit)
		}
		ctxt.MemLimit = mlimit
	} else {
		ctxt.MemLimit = memAvailable
	}

	if memAvailable < ctxt.MemLimit {
		ctxt.MemLimit = memAvailable
	}

	ctxt.FTP = ctx.StringSlice("ftp")
	ctxt.SFTP = ctx.StringSlice("sftp")
	ctxt.Interface = ctx.String("interface")
	ctxt.UserTimeout = ctx.Duration("conn-user-timeout")
	ctxt.SendBufSize = ctx.Int("send-buf-size")
	ctxt.RecvBufSize = ctx.Int("recv-buf-size")
	ctxt.IdleTimeout = ctx.Duration("idle-timeout")
	ctxt.UserTimeout = ctx.Duration("conn-user-timeout")

	if conf := ctx.String("config"); len(conf) > 0 {
		err = mergeServerCtxtFromConfigFile(conf, ctxt)
	} else {
		err = mergeDisksLayoutFromArgs(serverCmdArgs(ctx), ctxt)
	}

	return err
}

func handleCommonArgs(ctxt serverCtxt) {
	if ctxt.JSON {
		logger.EnableJSON()
	}
	if ctxt.Quiet {
		logger.EnableQuiet()
	}
	if ctxt.Anonymous {
		logger.EnableAnonymous()
	}

	consoleAddr := ctxt.ConsoleAddr
	addr := ctxt.Addr
	configDir := ctxt.ConfigDir
	configSet := ctxt.configDirSet
	certsDir := ctxt.CertsDir
	certsSet := ctxt.certsDirSet

	if globalBrowserEnabled {
		if consoleAddr == "" {
			p, err := xnet.GetFreePort()
			if err != nil {
				logger.FatalIf(err, "Unable to get free port for Console UI on the host")
			}
			// hold the port
			l, err := net.Listen("TCP", fmt.Sprintf(":%s", p.String()))
			if err == nil {
				defer l.Close()
			}
			consoleAddr = net.JoinHostPort("", p.String())
		}

		if _, _, err := net.SplitHostPort(consoleAddr); err != nil {
			logger.FatalIf(err, "Unable to start listening on console port")
		}

		if consoleAddr == addr {
			logger.FatalIf(errors.New("--console-address cannot be same as --address"), "Unable to start the server")
		}
	}

	globalMinioHost, globalMinioPort = mustSplitHostPort(addr)
	if globalMinioPort == "0" {
		p, err := xnet.GetFreePort()
		if err != nil {
			logger.FatalIf(err, "Unable to get free port for S3 API on the host")
		}
		globalMinioPort = p.String()
		globalDynamicAPIPort = true
	}

	if globalBrowserEnabled {
		globalMinioConsoleHost, globalMinioConsolePort = mustSplitHostPort(consoleAddr)
	}

	if globalMinioPort == globalMinioConsolePort {
		logger.FatalIf(errors.New("--console-address port cannot be same as --address port"), "Unable to start the server")
	}

	globalMinioAddr = addr

	// Set all config, certs and CAs directories.
	var err error
	globalConfigDir, err = newConfigDir(configDir, configSet, defaultConfigDir.Get)
	logger.FatalIf(err, "Unable to initialize the (deprecated) config directory")
	globalCertsDir, err = newConfigDir(certsDir, certsSet, defaultCertsDir.Get)
	logger.FatalIf(err, "Unable to initialize the certs directory")

	// Remove this code when we deprecate and remove config-dir.
	// This code is to make sure we inherit from the config-dir
	// option if certs-dir is not provided.
	if !certsSet && configSet {
		globalCertsDir = &ConfigDir{path: filepath.Join(globalConfigDir.Get(), certsDir)}
	}

	globalCertsCADir = &ConfigDir{path: filepath.Join(globalCertsDir.Get(), certsCADir)}

	logger.FatalIf(mkdirAllIgnorePerm(globalCertsCADir.Get()), "Unable to create certs CA directory at %s", globalCertsCADir.Get())
}

func runDNSCache(ctx *cli.Context) {
	dnsTTL := ctx.Duration("dns-cache-ttl")
	// Check if we have configured a custom DNS cache TTL.
	if dnsTTL <= 0 {
		if orchestrated {
			dnsTTL = 30 * time.Second
		} else {
			dnsTTL = 10 * time.Minute
		}
	}

	// Call to refresh will refresh names in cache.
	go func() {
		// Baremetal setups set DNS refresh window up to dnsTTL duration.
		t := time.NewTicker(dnsTTL)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				globalDNSCache.Refresh()

			case <-GlobalContext.Done():
				return
			}
		}
	}()
}

type envKV struct {
	Key   string
	Value string
	Skip  bool
}

func (e envKV) String() string {
	if e.Skip {
		return ""
	}
	return fmt.Sprintf("%s=%s", e.Key, e.Value)
}

func parsEnvEntry(envEntry string) (envKV, error) {
	envEntry = strings.TrimSpace(envEntry)
	if envEntry == "" {
		// Skip all empty lines
		return envKV{
			Skip: true,
		}, nil
	}
	if strings.HasPrefix(envEntry, "#") {
		// Skip commented lines
		return envKV{
			Skip: true,
		}, nil
	}
	envTokens := strings.SplitN(strings.TrimSpace(strings.TrimPrefix(envEntry, "export")), config.EnvSeparator, 2)
	if len(envTokens) != 2 {
		return envKV{}, fmt.Errorf("envEntry malformed; %s, expected to be of form 'KEY=value'", envEntry)
	}

	key := envTokens[0]
	val := envTokens[1]

	// Remove quotes from the value if found
	if len(val) >= 2 {
		quote := val[0]
		if (quote == '"' || quote == '\'') && val[len(val)-1] == quote {
			val = val[1 : len(val)-1]
		}
	}

	return envKV{
		Key:   key,
		Value: val,
	}, nil
}

// Similar to os.Environ returns a copy of strings representing
// the environment values from a file, in the form "key, value".
// in a structured form.
func minioEnvironFromFile(envConfigFile string) ([]envKV, error) {
	f, err := Open(envConfigFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ekvs []envKV
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		ekv, err := parsEnvEntry(scanner.Text())
		if err != nil {
			return nil, err
		}
		if ekv.Skip {
			// Skips empty lines
			continue
		}
		ekvs = append(ekvs, ekv)
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	return ekvs, nil
}

func readFromSecret(sp string) (string, error) {
	// Supports reading path from docker secrets, filename is
	// relative to /run/secrets/ position.
	if isFile(pathJoin("/run/secrets/", sp)) {
		sp = pathJoin("/run/secrets/", sp)
	}
	credBuf, err := os.ReadFile(sp)
	if err != nil {
		if os.IsNotExist(err) { // ignore if file doesn't exist.
			return "", nil
		}
		return "", err
	}
	return string(bytes.TrimSpace(credBuf)), nil
}

func loadEnvVarsFromFiles() {
	if env.IsSet(config.EnvAccessKeyFile) {
		accessKey, err := readFromSecret(env.Get(config.EnvAccessKeyFile, ""))
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the secret file(s)")
		}
		if accessKey != "" {
			os.Setenv(config.EnvRootUser, accessKey)
		}
	}

	if env.IsSet(config.EnvSecretKeyFile) {
		secretKey, err := readFromSecret(env.Get(config.EnvSecretKeyFile, ""))
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the secret file(s)")
		}
		if secretKey != "" {
			os.Setenv(config.EnvRootPassword, secretKey)
		}
	}

	if env.IsSet(config.EnvRootUserFile) {
		rootUser, err := readFromSecret(env.Get(config.EnvRootUserFile, ""))
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the secret file(s)")
		}
		if rootUser != "" {
			os.Setenv(config.EnvRootUser, rootUser)
		}
	}

	if env.IsSet(config.EnvRootPasswordFile) {
		rootPassword, err := readFromSecret(env.Get(config.EnvRootPasswordFile, ""))
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the secret file(s)")
		}
		if rootPassword != "" {
			os.Setenv(config.EnvRootPassword, rootPassword)
		}
	}

	if env.IsSet(config.EnvConfigEnvFile) {
		ekvs, err := minioEnvironFromFile(env.Get(config.EnvConfigEnvFile, ""))
		if err != nil && !os.IsNotExist(err) {
			logger.Fatal(err, "Unable to read the config environment file")
		}
		for _, ekv := range ekvs {
			os.Setenv(ekv.Key, ekv.Value)
		}
	}
}

func serverHandleEarlyEnvVars() {
	var err error
	globalBrowserEnabled, err = config.ParseBool(env.Get(config.EnvBrowser, config.EnableOn))
	if err != nil {
		logger.Fatal(config.ErrInvalidBrowserValue(err), "Invalid MINIO_BROWSER value in environment variable")
	}
}

func serverHandleEnvVars() {
	var err error
	if globalBrowserEnabled {
		if redirectURL := env.Get(config.EnvBrowserRedirectURL, ""); redirectURL != "" {
			u, err := xnet.ParseHTTPURL(redirectURL)
			if err != nil {
				logger.Fatal(err, "Invalid MINIO_BROWSER_REDIRECT_URL value in environment variable")
			}
			// Look for if URL has invalid values and return error.
			if !isValidURLEndpoint((*url.URL)(u)) {
				err := fmt.Errorf("URL contains unexpected resources, expected URL to be one of http(s)://console.example.com or as a subpath via API endpoint http(s)://minio.example.com/minio format: %v", u)
				logger.Fatal(err, "Invalid MINIO_BROWSER_REDIRECT_URL value is environment variable")
			}
			globalBrowserRedirectURL = u
		}
		globalBrowserRedirect = env.Get(config.EnvBrowserRedirect, config.EnableOn) == config.EnableOn
	}

	if serverURL := env.Get(config.EnvMinIOServerURL, ""); serverURL != "" {
		u, err := xnet.ParseHTTPURL(serverURL)
		if err != nil {
			logger.Fatal(err, "Invalid MINIO_SERVER_URL value in environment variable")
		}
		// Look for if URL has invalid values and return error.
		if !isValidURLEndpoint((*url.URL)(u)) {
			err := fmt.Errorf("URL contains unexpected resources, expected URL to be of http(s)://minio.example.com format: %v", u)
			logger.Fatal(err, "Invalid MINIO_SERVER_URL value is environment variable")
		}
		u.Path = "" // remove any path component such as `/`
		globalMinioEndpoint = u.String()
		globalMinioEndpointURL = u
	}

	globalFSOSync, err = config.ParseBool(env.Get(config.EnvFSOSync, config.EnableOff))
	if err != nil {
		logger.Fatal(config.ErrInvalidFSOSyncValue(err), "Invalid MINIO_FS_OSYNC value in environment variable")
	}

	rootDiskSize := env.Get(config.EnvRootDriveThresholdSize, "")
	if rootDiskSize == "" {
		rootDiskSize = env.Get(config.EnvRootDiskThresholdSize, "")
	}
	if rootDiskSize != "" {
		size, err := humanize.ParseBytes(rootDiskSize)
		if err != nil {
			logger.Fatal(err, fmt.Sprintf("Invalid %s value in root drive threshold environment variable", rootDiskSize))
		}
		globalRootDiskThreshold = size
	}

	domains := env.Get(config.EnvDomain, "")
	if len(domains) != 0 {
		for domainName := range strings.SplitSeq(domains, config.ValueSeparator) {
			if _, ok := dns2.IsDomainName(domainName); !ok {
				logger.Fatal(config.ErrInvalidDomainValue(nil).Msgf("Unknown value `%s`", domainName),
					"Invalid MINIO_DOMAIN value in environment variable")
			}
			globalDomainNames = append(globalDomainNames, domainName)
		}
		sort.Strings(globalDomainNames)
		lcpSuf := lcpSuffix(globalDomainNames)
		for _, domainName := range globalDomainNames {
			if domainName == lcpSuf && len(globalDomainNames) > 1 {
				logger.Fatal(config.ErrOverlappingDomainValue(nil).Msgf("Overlapping domains `%s` not allowed", globalDomainNames),
					"Invalid MINIO_DOMAIN value in environment variable")
			}
		}
	}

	publicIPs := env.Get(config.EnvPublicIPs, "")
	if len(publicIPs) != 0 {
		minioEndpoints := strings.Split(publicIPs, config.ValueSeparator)
		domainIPs := set.NewStringSet()
		for _, endpoint := range minioEndpoints {
			if net.ParseIP(endpoint) == nil {
				// Checking if the IP is a DNS entry.
				addrs, err := globalDNSCache.LookupHost(GlobalContext, endpoint)
				if err != nil {
					logger.FatalIf(err, "Unable to initialize MinIO server with [%s] invalid entry found in MINIO_PUBLIC_IPS", endpoint)
				}
				for _, addr := range addrs {
					domainIPs.Add(addr)
				}
			}
			domainIPs.Add(endpoint)
		}
		updateDomainIPs(domainIPs)
	} else {
		// Add found interfaces IP address to global domain IPS,
		// loopback addresses will be naturally dropped.
		domainIPs := mustGetLocalIP4()
		for _, host := range globalEndpoints.Hostnames() {
			domainIPs.Add(host)
		}
		updateDomainIPs(domainIPs)
	}

	// In place update is true by default if the MINIO_UPDATE is not set
	// or is not set to 'off', if MINIO_UPDATE is set to 'off' then
	// in-place update is off.
	globalInplaceUpdateDisabled = strings.EqualFold(env.Get(config.EnvUpdate, config.EnableOn), config.EnableOff)

	// Check if the supported credential env vars,
	// "MINIO_ROOT_USER" and "MINIO_ROOT_PASSWORD" are provided
	// Warn user if deprecated environment variables,
	// "MINIO_ACCESS_KEY" and "MINIO_SECRET_KEY", are defined
	// Check all error conditions first
	//nolint:gocritic
	if !env.IsSet(config.EnvRootUser) && env.IsSet(config.EnvRootPassword) {
		logger.Fatal(config.ErrMissingEnvCredentialRootUser(nil), "Unable to start MinIO")
	} else if env.IsSet(config.EnvRootUser) && !env.IsSet(config.EnvRootPassword) {
		logger.Fatal(config.ErrMissingEnvCredentialRootPassword(nil), "Unable to start MinIO")
	} else if !env.IsSet(config.EnvRootUser) && !env.IsSet(config.EnvRootPassword) {
		if !env.IsSet(config.EnvAccessKey) && env.IsSet(config.EnvSecretKey) {
			logger.Fatal(config.ErrMissingEnvCredentialAccessKey(nil), "Unable to start MinIO")
		} else if env.IsSet(config.EnvAccessKey) && !env.IsSet(config.EnvSecretKey) {
			logger.Fatal(config.ErrMissingEnvCredentialSecretKey(nil), "Unable to start MinIO")
		}
	}

	globalEnableSyncBoot = env.Get("MINIO_SYNC_BOOT", config.EnableOff) == config.EnableOn
}

func loadRootCredentials() auth.Credentials {
	// At this point, either both environment variables
	// are defined or both are not defined.
	// Check both cases and authenticate them if correctly defined
	var user, password string
	var legacyCredentials bool
	//nolint:gocritic
	if env.IsSet(config.EnvRootUser) && env.IsSet(config.EnvRootPassword) {
		user = env.Get(config.EnvRootUser, "")
		password = env.Get(config.EnvRootPassword, "")
	} else if env.IsSet(config.EnvAccessKey) && env.IsSet(config.EnvSecretKey) {
		user = env.Get(config.EnvAccessKey, "")
		password = env.Get(config.EnvSecretKey, "")
		legacyCredentials = true
	} else if globalServerCtxt.RootUser != "" && globalServerCtxt.RootPwd != "" {
		user, password = globalServerCtxt.RootUser, globalServerCtxt.RootPwd
	}
	if user == "" || password == "" {
		return auth.Credentials{}
	}
	cred, err := auth.CreateCredentials(user, password)
	if err != nil {
		if legacyCredentials {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the shell environment")
		} else {
			logger.Fatal(config.ErrInvalidRootUserCredentials(err),
				"Unable to validate credentials inherited from the shell environment")
		}
	}
	if env.IsSet(config.EnvAccessKey) && env.IsSet(config.EnvSecretKey) {
		msg := fmt.Sprintf("WARNING: %s and %s are deprecated.\n"+
			"         Please use %s and %s",
			config.EnvAccessKey, config.EnvSecretKey,
			config.EnvRootUser, config.EnvRootPassword)
		logger.Info(color.RedBold(msg))
	}
	globalCredViaEnv = true
	return cred
}

// autoGenerateRootCredentials generates root credentials deterministically if
// a KMS is configured, no manual credentials have been specified and if root
// access is disabled.
func autoGenerateRootCredentials() auth.Credentials {
	if GlobalKMS == nil {
		return globalActiveCred
	}

	aKey, err := GlobalKMS.MAC(GlobalContext, &kms.MACRequest{Message: []byte("root access key")})
	if IsErrIgnored(err, kes.ErrNotAllowed, kms.ErrNotSupported, errors.ErrUnsupported, kms.ErrPermission) {
		// If we don't have permission to compute the HMAC, don't change the cred.
		return globalActiveCred
	}
	if err != nil {
		logger.Fatal(err, "Unable to generate root access key using KMS")
	}

	sKey, err := GlobalKMS.MAC(GlobalContext, &kms.MACRequest{Message: []byte("root secret key")})
	if err != nil {
		// Here, we must have permission. Otherwise, we would have failed earlier.
		logger.Fatal(err, "Unable to generate root secret key using KMS")
	}

	accessKey, err := auth.GenerateAccessKey(20, bytes.NewReader(aKey))
	if err != nil {
		logger.Fatal(err, "Unable to generate root access key")
	}
	secretKey, err := auth.GenerateSecretKey(32, bytes.NewReader(sKey))
	if err != nil {
		logger.Fatal(err, "Unable to generate root secret key")
	}

	logger.Info("Automatically generated root access key and secret key with the KMS")
	return auth.Credentials{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
}

// Initialize KMS global variable after valiadating and loading the configuration.
// It depends on KMS env variables and global cli flags.
func handleKMSConfig() {
	present, err := kms.IsPresent()
	if err != nil {
		logger.Fatal(err, "Invalid KMS configuration specified")
	}
	if !present {
		return
	}

	KMS, err := kms.Connect(GlobalContext, &kms.ConnectionOptions{
		CADir: globalCertsCADir.Get(),
	})
	if err != nil {
		logger.Fatal(err, "Failed to connect to KMS")
	}

	if _, err = KMS.GenerateKey(GlobalContext, &kms.GenerateKeyRequest{}); errors.Is(err, kms.ErrKeyNotFound) {
		err = KMS.CreateKey(GlobalContext, &kms.CreateKeyRequest{Name: KMS.DefaultKey})
	}
	if err != nil && !errors.Is(err, kms.ErrKeyExists) && !errors.Is(err, kms.ErrPermission) {
		logger.Fatal(err, "Failed to connect to KMS")
	}
	GlobalKMS = KMS
}

func getTLSConfig() (x509Certs []*x509.Certificate, manager *certs.Manager, secureConn bool, err error) {
	if !isFile(getPublicCertFile()) || !isFile(getPrivateKeyFile()) {
		return nil, nil, false, nil
	}

	if x509Certs, err = config.ParsePublicCertFile(getPublicCertFile()); err != nil {
		return nil, nil, false, err
	}

	manager, err = certs.NewManager(GlobalContext, getPublicCertFile(), getPrivateKeyFile(), config.LoadX509KeyPair)
	if err != nil {
		return nil, nil, false, err
	}

	// MinIO has support for multiple certificates. It expects the following structure:
	//  certs/
	//   │
	//   ├─ public.crt
	//   ├─ private.key
	//   │
	//   ├─ example.com/
	//   │   │
	//   │   ├─ public.crt
	//   │   └─ private.key
	//   └─ foobar.org/
	//      │
	//      ├─ public.crt
	//      └─ private.key
	//   ...
	//
	// Therefore, we read all filenames in the cert directory and check
	// for each directory whether it contains a public.crt and private.key.
	// If so, we try to add it to certificate manager.
	root, err := Open(globalCertsDir.Get())
	if err != nil {
		return nil, nil, false, err
	}
	defer root.Close()

	files, err := root.Readdir(-1)
	if err != nil {
		return nil, nil, false, err
	}
	for _, file := range files {
		// Ignore all
		// - regular files
		// - "CAs" directory
		// - any directory which starts with ".."
		if file.Mode().IsRegular() || file.Name() == "CAs" || strings.HasPrefix(file.Name(), "..") {
			continue
		}
		if file.Mode()&os.ModeSymlink == os.ModeSymlink {
			file, err = Stat(filepath.Join(root.Name(), file.Name()))
			if err != nil {
				// not accessible ignore
				continue
			}
			if !file.IsDir() {
				continue
			}
		}

		var (
			certFile = filepath.Join(root.Name(), file.Name(), publicCertFile)
			keyFile  = filepath.Join(root.Name(), file.Name(), privateKeyFile)
		)
		if !isFile(certFile) || !isFile(keyFile) {
			continue
		}
		if err = manager.AddCertificate(certFile, keyFile); err != nil {
			err = fmt.Errorf("Unable to load TLS certificate '%s,%s': %w", certFile, keyFile, err)
			bootLogIf(GlobalContext, err, logger.ErrorKind)
		}
	}
	secureConn = true

	// Certs might be symlinks, reload them every 10 seconds.
	manager.UpdateReloadDuration(10 * time.Second)

	// syscall.SIGHUP to reload the certs.
	manager.ReloadOnSignal(syscall.SIGHUP)

	return x509Certs, manager, secureConn, nil
}

// contextCanceled returns whether a context is canceled.
func contextCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// bgContext returns a context that can be used for async operations.
// Cancellation/timeouts are removed, so parent cancellations/timeout will
// not propagate from parent.
// Context values are preserved.
// This can be used for goroutines that live beyond the parent context.
func bgContext(parent context.Context) context.Context {
	return bgCtx{parent: parent}
}

type bgCtx struct {
	parent context.Context
}

func (a bgCtx) Done() <-chan struct{} {
	return nil
}

func (a bgCtx) Err() error {
	return nil
}

func (a bgCtx) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (a bgCtx) Value(key any) any {
	return a.parent.Value(key)
}
