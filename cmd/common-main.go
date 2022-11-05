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
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"encoding/pem"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
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
	consoleoauth2 "github.com/minio/console/pkg/auth/idp/oauth2"
	consoleCerts "github.com/minio/console/pkg/certs"
	"github.com/minio/console/restapi"
	"github.com/minio/console/restapi/operations"
	"github.com/minio/kes"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/certs"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/ellipses"
	"github.com/minio/pkg/env"
	xnet "github.com/minio/pkg/net"
	"github.com/rs/dnscache"
)

// serverDebugLog will enable debug printing
var serverDebugLog = env.Get("_MINIO_SERVER_DEBUG", config.EnableOff) == config.EnableOn

var (
	shardDiskTimeDelta     time.Duration
	defaultAWSCredProvider []credentials.Provider
)

func init() {
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

	rand.Seed(time.Now().UTC().UnixNano())

	logger.Init(GOPATH, GOROOT)
	logger.RegisterError(config.FmtError)

	initGlobalContext()

	options := dnscache.ResolverRefreshOptions{
		ClearUnused:      true,
		PersistOnFailure: false,
	}

	t, _ := minioVersionToReleaseTime(Version)
	if !t.IsZero() {
		globalVersionUnix = uint64(t.Unix())
	}

	globalIsCICD = env.Get("MINIO_CI_CD", "") != "" || env.Get("CI", "") != ""

	containers := IsKubernetes() || IsDocker() || IsBOSH() || IsDCOS() || IsPCFTile()

	// Call to refresh will refresh names in cache. If you pass true, it will also
	// remove cached names not looked up since the last call to Refresh. It is a good idea
	// to call this method on a regular interval.
	go func() {
		var t *time.Ticker
		if containers {
			t = time.NewTicker(1 * time.Minute)
		} else {
			t = time.NewTicker(10 * time.Minute)
		}
		defer t.Stop()
		for {
			select {
			case <-t.C:
				globalDNSCache.RefreshWithOptions(options)
			case <-GlobalContext.Done():
				return
			}
		}
	}()

	globalForwarder = handlers.NewForwarder(&handlers.Forwarder{
		PassHost:     true,
		RoundTripper: newHTTPTransport(1 * time.Hour),
		Logger: func(err error) {
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.LogIf(GlobalContext, err)
			}
		},
	})

	console.SetColor("Debug", fcolor.New())

	gob.Register(StorageErr(""))
	gob.Register(madmin.TimeInfo{})
	gob.Register(map[string]interface{}{})

	defaultAWSCredProvider = []credentials.Provider{
		&credentials.IAM{
			Client: &http.Client{
				Transport: NewHTTPTransport(),
			},
		},
	}

	var err error
	shardDiskTimeDelta, err = time.ParseDuration(env.Get("_MINIO_SHARD_DISKTIME_DELTA", "1m"))
	if err != nil {
		shardDiskTimeDelta = 1 * time.Minute
	}

	// All minio-go API operations shall be performed only once,
	// another way to look at this is we are turning off retries.
	minio.MaxRetry = 1
}

const consolePrefix = "CONSOLE_"

func minioConfigToConsoleFeatures() {
	os.Setenv("CONSOLE_PBKDF_SALT", globalDeploymentID)
	os.Setenv("CONSOLE_PBKDF_PASSPHRASE", globalDeploymentID)
	if globalMinioEndpoint != "" {
		os.Setenv("CONSOLE_MINIO_SERVER", globalMinioEndpoint)
	} else {
		// Explicitly set 127.0.0.1 so Console will automatically bypass TLS verification to the local S3 API.
		// This will save users from providing a certificate with IP or FQDN SAN that points to the local host.
		os.Setenv("CONSOLE_MINIO_SERVER", fmt.Sprintf("%s://127.0.0.1:%s", getURLScheme(globalIsTLS), globalMinioPort))
	}
	if value := env.Get("MINIO_LOG_QUERY_URL", ""); value != "" {
		os.Setenv("CONSOLE_LOG_QUERY_URL", value)
		if value := env.Get("MINIO_LOG_QUERY_AUTH_TOKEN", ""); value != "" {
			os.Setenv("CONSOLE_LOG_QUERY_AUTH_TOKEN", value)
		}
	}
	// pass the console subpath configuration
	if value := env.Get(config.EnvMinIOBrowserRedirectURL, ""); value != "" {
		subPath := path.Clean(pathJoin(strings.TrimSpace(globalBrowserRedirectURL.Path), SlashSeparator))
		if subPath != SlashSeparator {
			os.Setenv("CONSOLE_SUBPATH", subPath)
		}
	}
	// Enable if prometheus URL is set.
	if value := env.Get("MINIO_PROMETHEUS_URL", ""); value != "" {
		os.Setenv("CONSOLE_PROMETHEUS_URL", value)
		if value := env.Get("MINIO_PROMETHEUS_JOB_ID", "minio-job"); value != "" {
			os.Setenv("CONSOLE_PROMETHEUS_JOB_ID", value)
			// Support additional labels for more granular filtering.
			if value := env.Get("MINIO_PROMETHEUS_EXTRA_LABELS", ""); value != "" {
				os.Setenv("CONSOLE_PROMETHEUS_EXTRA_LABELS", value)
			}
		}
	}
	// Enable if LDAP is enabled.
	if globalLDAPConfig.Enabled() {
		os.Setenv("CONSOLE_LDAP_ENABLED", config.EnableOn)
	}
	os.Setenv("CONSOLE_MINIO_REGION", globalSite.Region)
	os.Setenv("CONSOLE_CERT_PASSWD", env.Get("MINIO_CERT_PASSWD", ""))

	globalSubnetConfig.ApplyEnv()
}

func buildOpenIDConsoleConfig() consoleoauth2.OpenIDPCfg {
	m := make(map[string]consoleoauth2.ProviderConfig, len(globalOpenIDConfig.ProviderCfgs))
	for name, cfg := range globalOpenIDConfig.ProviderCfgs {
		callback := getConsoleEndpoints()[0] + "/oauth_callback"
		if cfg.RedirectURI != "" {
			callback = cfg.RedirectURI
		}
		m[name] = consoleoauth2.ProviderConfig{
			URL:                     cfg.URL.String(),
			DisplayName:             cfg.DisplayName,
			ClientID:                cfg.ClientID,
			ClientSecret:            cfg.ClientSecret,
			HMACSalt:                globalDeploymentID,
			HMACPassphrase:          cfg.ClientID,
			Scopes:                  strings.Join(cfg.DiscoveryDoc.ScopesSupported, ","),
			Userinfo:                cfg.ClaimUserinfo,
			RedirectCallbackDynamic: cfg.RedirectURIDynamic,
			RedirectCallback:        callback,
			RoleArn:                 cfg.GetRoleArn(),
		}
	}
	return m
}

func initConsoleServer() (*restapi.Server, error) {
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

	swaggerSpec, err := loads.Embedded(restapi.SwaggerJSON, restapi.FlatSwaggerJSON)
	if err != nil {
		return nil, err
	}

	api := operations.NewConsoleAPI(swaggerSpec)

	if !serverDebugLog {
		// Disable console logging if server debug log is not enabled
		noLog := func(string, ...interface{}) {}

		restapi.LogInfo = noLog
		restapi.LogError = noLog
		api.Logger = noLog
	}

	// Pass in console application config. This needs to happen before the
	// ConfigureAPI() call.
	restapi.GlobalMinIOConfig = restapi.MinIOConfig{
		OpenIDProviders: buildOpenIDConsoleConfig(),
	}

	server := restapi.NewServer(api)
	// register all APIs
	server.ConfigureAPI()

	restapi.GlobalRootCAs, restapi.GlobalPublicCerts, restapi.GlobalTLSCertsManager = globalRootCAs, globalPublicCerts, globalTLSCerts

	consolePort, _ := strconv.Atoi(globalMinioConsolePort)

	server.Host = globalMinioConsoleHost
	server.Port = consolePort
	restapi.Port = globalMinioConsolePort
	restapi.Hostname = globalMinioConsoleHost

	if globalIsTLS {
		// If TLS certificates are provided enforce the HTTPS.
		server.EnabledListeners = []string{"https"}
		server.TLSPort = consolePort
		// Need to store tls-port, tls-host un config variables so secure.middleware can read from there
		restapi.TLSPort = globalMinioConsolePort
		restapi.Hostname = globalMinioConsoleHost
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

	// Its OK to ignore any errors during doUpdate() here.
	crTime, err := GetCurrentReleaseTime()
	if err != nil {
		return
	}

	_, lrTime, err := getLatestReleaseTime(u, 2*time.Second, mode)
	if err != nil {
		return
	}

	var older time.Duration
	var downloadURL string
	if lrTime.After(crTime) {
		older = lrTime.Sub(crTime)
		downloadURL = getDownloadURL(releaseTimeToReleaseTag(lrTime))
	}

	updateMsg := prepareUpdateMessage(downloadURL, older)
	if updateMsg == "" {
		return
	}

	logger.Info(prepareUpdateMessage("Run `mc admin update`", lrTime.Sub(crTime)))
}

func newConfigDirFromCtx(ctx *cli.Context, option string, getDefaultDir func() string) (*ConfigDir, bool) {
	var dir string
	var dirSet bool

	switch {
	case ctx.IsSet(option):
		dir = ctx.String(option)
		dirSet = true
	case ctx.GlobalIsSet(option):
		dir = ctx.GlobalString(option)
		dirSet = true
		// cli package does not expose parent's option option.  Below code is workaround.
		if dir == "" || dir == getDefaultDir() {
			dirSet = false // Unset to false since GlobalIsSet() true is a false positive.
			if ctx.Parent().GlobalIsSet(option) {
				dir = ctx.Parent().GlobalString(option)
				dirSet = true
			}
		}
	default:
		// Neither local nor global option is provided.  In this case, try to use
		// default directory.
		dir = getDefaultDir()
		if dir == "" {
			logger.FatalIf(errInvalidArgument, "%s option must be provided", option)
		}
	}

	if dir == "" {
		logger.FatalIf(errors.New("empty directory"), "%s directory cannot be empty", option)
	}

	// Disallow relative paths, figure out absolute paths.
	dirAbs, err := filepath.Abs(dir)
	logger.FatalIf(err, "Unable to fetch absolute path for %s=%s", option, dir)

	logger.FatalIf(mkdirAllIgnorePerm(dirAbs), "Unable to create directory specified %s=%s", option, dir)

	return &ConfigDir{path: dirAbs}, dirSet
}

func handleCommonCmdArgs(ctx *cli.Context) {
	// Get "json" flag from command line argument and
	// enable json and quite modes if json flag is turned on.
	globalCLIContext.JSON = ctx.IsSet("json") || ctx.GlobalIsSet("json")
	if globalCLIContext.JSON {
		logger.EnableJSON()
	}

	// Get quiet flag from command line argument.
	globalCLIContext.Quiet = ctx.IsSet("quiet") || ctx.GlobalIsSet("quiet")
	if globalCLIContext.Quiet {
		logger.EnableQuiet()
	}

	// Get anonymous flag from command line argument.
	globalCLIContext.Anonymous = ctx.IsSet("anonymous") || ctx.GlobalIsSet("anonymous")
	if globalCLIContext.Anonymous {
		logger.EnableAnonymous()
	}

	// Fetch address option
	addr := ctx.GlobalString("address")
	if addr == "" || addr == ":"+GlobalMinioDefaultPort {
		addr = ctx.String("address")
	}

	// Fetch console address option
	consoleAddr := ctx.GlobalString("console-address")
	if consoleAddr == "" {
		consoleAddr = ctx.String("console-address")
	}

	if consoleAddr == "" {
		p, err := xnet.GetFreePort()
		if err != nil {
			logger.FatalIf(err, "Unable to get free port for console on the host")
		}
		consoleAddr = net.JoinHostPort("", p.String())
	}

	if _, _, err := net.SplitHostPort(consoleAddr); err != nil {
		logger.FatalIf(err, "Unable to start listening on console port")
	}

	if consoleAddr == addr {
		logger.FatalIf(errors.New("--console-address cannot be same as --address"), "Unable to start the server")
	}

	globalMinioHost, globalMinioPort = mustSplitHostPort(addr)
	globalMinioConsoleHost, globalMinioConsolePort = mustSplitHostPort(consoleAddr)

	if globalMinioPort == globalMinioConsolePort {
		logger.FatalIf(errors.New("--console-address port cannot be same as --address port"), "Unable to start the server")
	}

	globalMinioAddr = addr

	// Check "no-compat" flag from command line argument.
	globalCLIContext.StrictS3Compat = true
	if ctx.IsSet("no-compat") || ctx.GlobalIsSet("no-compat") {
		globalCLIContext.StrictS3Compat = false
	}

	// Set all config, certs and CAs directories.
	var configSet, certsSet bool
	globalConfigDir, configSet = newConfigDirFromCtx(ctx, "config-dir", defaultConfigDir.Get)
	globalCertsDir, certsSet = newConfigDirFromCtx(ctx, "certs-dir", defaultCertsDir.Get)

	// Remove this code when we deprecate and remove config-dir.
	// This code is to make sure we inherit from the config-dir
	// option if certs-dir is not provided.
	if !certsSet && configSet {
		globalCertsDir = &ConfigDir{path: filepath.Join(globalConfigDir.Get(), certsDir)}
	}

	globalCertsCADir = &ConfigDir{path: filepath.Join(globalCertsDir.Get(), certsCADir)}

	logger.FatalIf(mkdirAllIgnorePerm(globalCertsCADir.Get()), "Unable to create certs CA directory at %s", globalCertsCADir.Get())
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

	if env.IsSet(config.EnvKMSSecretKeyFile) {
		kmsSecret, err := readFromSecret(env.Get(config.EnvKMSSecretKeyFile, ""))
		if err != nil {
			logger.Fatal(err, "Unable to read the KMS secret key inherited from secret file")
		}
		if kmsSecret != "" {
			os.Setenv(config.EnvKMSSecretKey, kmsSecret)
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

func handleCommonEnvVars() {
	loadEnvVarsFromFiles()

	var err error
	globalBrowserEnabled, err = config.ParseBool(env.Get(config.EnvBrowser, config.EnableOn))
	if err != nil {
		logger.Fatal(config.ErrInvalidBrowserValue(err), "Invalid MINIO_BROWSER value in environment variable")
	}
	if globalBrowserEnabled {
		if redirectURL := env.Get(config.EnvMinIOBrowserRedirectURL, ""); redirectURL != "" {
			u, err := xnet.ParseHTTPURL(redirectURL)
			if err != nil {
				logger.Fatal(err, "Invalid MINIO_BROWSER_REDIRECT_URL value in environment variable")
			}
			// Look for if URL has invalid values and return error.
			if !((u.Scheme == "http" || u.Scheme == "https") &&
				u.Opaque == "" &&
				!u.ForceQuery && u.RawQuery == "" && u.Fragment == "") {
				err := fmt.Errorf("URL contains unexpected resources, expected URL to be of http(s)://minio.example.com format: %v", u)
				logger.Fatal(err, "Invalid MINIO_BROWSER_REDIRECT_URL value is environment variable")
			}
			globalBrowserRedirectURL = u
		}
	}

	if serverURL := env.Get(config.EnvMinIOServerURL, ""); serverURL != "" {
		u, err := xnet.ParseHTTPURL(serverURL)
		if err != nil {
			logger.Fatal(err, "Invalid MINIO_SERVER_URL value in environment variable")
		}
		// Look for if URL has invalid values and return error.
		if !((u.Scheme == "http" || u.Scheme == "https") &&
			(u.Path == "/" || u.Path == "") && u.Opaque == "" &&
			!u.ForceQuery && u.RawQuery == "" && u.Fragment == "") {
			err := fmt.Errorf("URL contains unexpected resources, expected URL to be of http(s)://minio.example.com format: %v", u)
			logger.Fatal(err, "Invalid MINIO_SERVER_URL value is environment variable")
		}
		u.Path = "" // remove any path component such as `/`
		globalMinioEndpoint = u.String()
	}

	globalFSOSync, err = config.ParseBool(env.Get(config.EnvFSOSync, config.EnableOff))
	if err != nil {
		logger.Fatal(config.ErrInvalidFSOSyncValue(err), "Invalid MINIO_FS_OSYNC value in environment variable")
	}

	if rootDiskSize := env.Get(config.EnvRootDiskThresholdSize, ""); rootDiskSize != "" {
		size, err := humanize.ParseBytes(rootDiskSize)
		if err != nil {
			logger.Fatal(err, fmt.Sprintf("Invalid %s value in environment variable", config.EnvRootDiskThresholdSize))
		}
		globalRootDiskThreshold = size
	}

	domains := env.Get(config.EnvDomain, "")
	if len(domains) != 0 {
		for _, domainName := range strings.Split(domains, config.ValueSeparator) {
			if _, ok := dns2.IsDomainName(domainName); !ok {
				logger.Fatal(config.ErrInvalidDomainValue(nil).Msg("Unknown value `%s`", domainName),
					"Invalid MINIO_DOMAIN value in environment variable")
			}
			globalDomainNames = append(globalDomainNames, domainName)
		}
		sort.Strings(globalDomainNames)
		lcpSuf := lcpSuffix(globalDomainNames)
		for _, domainName := range globalDomainNames {
			if domainName == lcpSuf && len(globalDomainNames) > 1 {
				logger.Fatal(config.ErrOverlappingDomainValue(nil).Msg("Overlapping domains `%s` not allowed", globalDomainNames),
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

	// At this point, either both environment variables
	// are defined or both are not defined.
	// Check both cases and authenticate them if correctly defined
	var user, password string
	var hasCredentials bool
	//nolint:gocritic
	if env.IsSet(config.EnvRootUser) && env.IsSet(config.EnvRootPassword) {
		user = env.Get(config.EnvRootUser, "")
		password = env.Get(config.EnvRootPassword, "")
		hasCredentials = true
	} else if env.IsSet(config.EnvAccessKey) && env.IsSet(config.EnvSecretKey) {
		user = env.Get(config.EnvAccessKey, "")
		password = env.Get(config.EnvSecretKey, "")
		hasCredentials = true
	}
	if hasCredentials {
		cred, err := auth.CreateCredentials(user, password)
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the shell environment")
		}
		if env.IsSet(config.EnvAccessKey) && env.IsSet(config.EnvSecretKey) {
			msg := fmt.Sprintf("WARNING: %s and %s are deprecated.\n"+
				"         Please use %s and %s",
				config.EnvAccessKey, config.EnvSecretKey,
				config.EnvRootUser, config.EnvRootPassword)
			logger.Info(color.RedBold(msg))
		}
		globalActiveCred = cred
	}
}

// Initialize KMS global variable after valiadating and loading the configuration.
// It depends on KMS env variables and global cli flags.
func handleKMSConfig() {
	switch {
	case env.IsSet(config.EnvKMSSecretKey) && env.IsSet(config.EnvKESEndpoint):
		logger.Fatal(errors.New("ambigious KMS configuration"), fmt.Sprintf("The environment contains %q as well as %q", config.EnvKMSSecretKey, config.EnvKESEndpoint))
	}

	if env.IsSet(config.EnvKMSSecretKey) {
		KMS, err := kms.Parse(env.Get(config.EnvKMSSecretKey, ""))
		if err != nil {
			logger.Fatal(err, "Unable to parse the KMS secret key inherited from the shell environment")
		}
		GlobalKMS = KMS
	}
	if env.IsSet(config.EnvKESEndpoint) {
		var endpoints []string
		for _, endpoint := range strings.Split(env.Get(config.EnvKESEndpoint, ""), ",") {
			if strings.TrimSpace(endpoint) == "" {
				continue
			}
			if !ellipses.HasEllipses(endpoint) {
				endpoints = append(endpoints, endpoint)
				continue
			}
			patterns, err := ellipses.FindEllipsesPatterns(endpoint)
			if err != nil {
				logger.Fatal(err, fmt.Sprintf("Invalid KES endpoint %q", endpoint))
			}
			for _, lbls := range patterns.Expand() {
				endpoints = append(endpoints, strings.Join(lbls, ""))
			}
		}
		rootCAs, err := certs.GetRootCAs(env.Get(config.EnvKESServerCA, globalCertsCADir.Get()))
		if err != nil {
			logger.Fatal(err, fmt.Sprintf("Unable to load X.509 root CAs for KES from %q", env.Get(config.EnvKESServerCA, globalCertsCADir.Get())))
		}

		loadX509KeyPair := func(certFile, keyFile string) (tls.Certificate, error) {
			// Manually load the certificate and private key into memory.
			// We need to check whether the private key is encrypted, and
			// if so, decrypt it using the user-provided password.
			certBytes, err := os.ReadFile(certFile)
			if err != nil {
				return tls.Certificate{}, fmt.Errorf("Unable to load KES client certificate as specified by the shell environment: %v", err)
			}
			keyBytes, err := os.ReadFile(keyFile)
			if err != nil {
				return tls.Certificate{}, fmt.Errorf("Unable to load KES client private key as specified by the shell environment: %v", err)
			}
			privateKeyPEM, rest := pem.Decode(bytes.TrimSpace(keyBytes))
			if len(rest) != 0 {
				return tls.Certificate{}, errors.New("Unable to load KES client private key as specified by the shell environment: private key contains additional data")
			}
			if x509.IsEncryptedPEMBlock(privateKeyPEM) {
				keyBytes, err = x509.DecryptPEMBlock(privateKeyPEM, []byte(env.Get(config.EnvKESClientPassword, "")))
				if err != nil {
					return tls.Certificate{}, fmt.Errorf("Unable to decrypt KES client private key as specified by the shell environment: %v", err)
				}
				keyBytes = pem.EncodeToMemory(&pem.Block{Type: privateKeyPEM.Type, Bytes: keyBytes})
			}
			certificate, err := tls.X509KeyPair(certBytes, keyBytes)
			if err != nil {
				return tls.Certificate{}, fmt.Errorf("Unable to load KES client certificate as specified by the shell environment: %v", err)
			}
			return certificate, nil
		}

		reloadCertEvents := make(chan tls.Certificate, 1)
		certificate, err := certs.NewCertificate(env.Get(config.EnvKESClientCert, ""), env.Get(config.EnvKESClientKey, ""), loadX509KeyPair)
		if err != nil {
			logger.Fatal(err, "Failed to load KES client certificate")
		}
		certificate.Watch(context.Background(), 15*time.Minute, syscall.SIGHUP)
		certificate.Notify(reloadCertEvents)

		defaultKeyID := env.Get(config.EnvKESKeyName, "")
		KMS, err := kms.NewWithConfig(kms.Config{
			Endpoints:        endpoints,
			DefaultKeyID:     defaultKeyID,
			Certificate:      certificate,
			ReloadCertEvents: reloadCertEvents,
			RootCAs:          rootCAs,
		})
		if err != nil {
			logger.Fatal(err, "Unable to initialize a connection to KES as specified by the shell environment")
		}

		// We check that the default key ID exists or try to create it otherwise.
		// This implicitly checks that we can communicate to KES. We don't treat
		// a policy error as failure condition since MinIO may not have the permission
		// to create keys - just to generate/decrypt data encryption keys.
		if err = KMS.CreateKey(context.Background(), defaultKeyID); err != nil && !errors.Is(err, kes.ErrKeyExists) && !errors.Is(err, kes.ErrNotAllowed) {
			logger.Fatal(err, "Unable to initialize a connection to KES as specified by the shell environment")
		}
		GlobalKMS = KMS
	}
}

func getTLSConfig() (x509Certs []*x509.Certificate, manager *certs.Manager, secureConn bool, err error) {
	if !(isFile(getPublicCertFile()) && isFile(getPrivateKeyFile())) {
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
			logger.LogIf(GlobalContext, err, logger.Minio)
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

func (a bgCtx) Value(key interface{}) interface{} {
	return a.parent.Value(key)
}
