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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	fcolor "github.com/fatih/color"
	"github.com/go-openapi/loads"
	dns2 "github.com/miekg/dns"
	"github.com/minio/cli"
	consoleCerts "github.com/minio/console/pkg/certs"
	"github.com/minio/console/restapi"
	"github.com/minio/console/restapi/operations"
	"github.com/minio/kes"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/handlers"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/certs"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/ellipses"
	"github.com/minio/pkg/env"
	xnet "github.com/minio/pkg/net"
)

// serverDebugLog will enable debug printing
var serverDebugLog = env.Get("_MINIO_SERVER_DEBUG", config.EnableOff) == config.EnableOn
var defaultAWSCredProvider []credentials.Provider

func init() {
	rand.Seed(time.Now().UTC().UnixNano())

	logger.Init(GOPATH, GOROOT)
	logger.RegisterError(config.FmtError)

	if IsKubernetes() || IsDocker() || IsBOSH() || IsDCOS() || IsKubernetesReplicaSet() || IsPCFTile() {
		// 30 seconds matches the orchestrator DNS TTLs, have
		// a 5 second timeout to lookup from DNS servers.
		globalDNSCache = xhttp.NewDNSCache(30*time.Second, 5*time.Second, logger.LogOnceIf)
	} else {
		// On bare-metals DNS do not change often, so it is
		// safe to assume a higher timeout upto 10 minutes.
		globalDNSCache = xhttp.NewDNSCache(10*time.Minute, 5*time.Second, logger.LogOnceIf)
	}
	initGlobalContext()

	globalForwarder = handlers.NewForwarder(&handlers.Forwarder{
		PassHost:     true,
		RoundTripper: newGatewayHTTPTransport(1 * time.Hour),
		Logger: func(err error) {
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.LogIf(GlobalContext, err)
			}
		},
	})

	globalTransitionState = newTransitionState()

	console.SetColor("Debug", fcolor.New())

	gob.Register(StorageErr(""))

	defaultAWSCredProvider = []credentials.Provider{
		&credentials.IAM{
			Client: &http.Client{
				Transport: NewGatewayHTTPTransport(),
			},
		},
	}
}

const consolePrefix = "CONSOLE_"

func minioConfigToConsoleFeatures() {
	os.Setenv("CONSOLE_PBKDF_PASSPHRASE", restapi.RandomCharString(16))
	os.Setenv("CONSOLE_PBKDF_SALT", restapi.RandomCharString(8))
	os.Setenv("CONSOLE_MINIO_SERVER", getAPIEndpoints()[0])
	if value := os.Getenv("MINIO_LOG_QUERY_URL"); value != "" {
		os.Setenv("CONSOLE_LOG_QUERY_URL", value)
	}
	if value := os.Getenv("MINIO_LOG_QUERY_AUTH_TOKEN"); value != "" {
		os.Setenv("CONSOLE_LOG_QUERY_AUTH_TOKEN", value)
	}
	// Enable if prometheus URL is set.
	if value := os.Getenv("MINIO_PROMETHEUS_URL"); value != "" {
		os.Setenv("CONSOLE_PROMETHEUS_URL", value)
	}
	// Enable if LDAP is enabled.
	if globalLDAPConfig.Enabled {
		os.Setenv("CONSOLE_LDAP_ENABLED", config.EnableOn)
	}
	// if IDP is enabled, set IDP environment variables
	if globalOpenIDConfig.URL != nil {
		os.Setenv("CONSOLE_IDP_URL", globalOpenIDConfig.DiscoveryDoc.Issuer)
		os.Setenv("CONSOLE_IDP_SCOPES", strings.Join(globalOpenIDConfig.DiscoveryDoc.ScopesSupported, ","))
		os.Setenv("CONSOLE_IDP_CLIENT_ID", globalOpenIDConfig.ClientID)
		os.Setenv("CONSOLE_IDP_SECRET", globalOpenIDConfig.ClientSecret)
	}
	os.Setenv("CONSOLE_MINIO_REGION", globalServerRegion)
	os.Setenv("CONSOLE_CERT_PASSWD", os.Getenv("MINIO_CERT_PASSWD"))
	os.Setenv("CONSOLE_IDP_CALLBACK", getConsoleEndpoints()[0]+"/oauth_callback")
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

	// Initialize MinIO loggers
	restapi.LogInfo = logger.Info
	restapi.LogError = logger.Error

	api := operations.NewConsoleAPI(swaggerSpec)
	api.Logger = func(_ string, _ ...interface{}) {
		// nothing to log.
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

	// subnet license refresh process
	go func() {
		// start refreshing subnet license after 5 seconds..
		time.Sleep(time.Second * 5)

		failedAttempts := 0
		for {
			if err := restapi.RefreshLicense(); err != nil {
				failedAttempts++
				// end license refresh after 3 consecutive failed attempts
				if failedAttempts >= 3 {
					return
				}
				// wait 5 minutes and retry again
				time.Sleep(time.Minute * 5)
				continue
			}
			// if license refreshed successfully reset the counter
			failedAttempts = 0
			// try to refresh license every 24 hrs
			time.Sleep(time.Hour * 24)
		}
	}()

	return server, nil
}

func verifyObjectLayerFeatures(name string, objAPI ObjectLayer) {
	if (GlobalKMS != nil) && !objAPI.IsEncryptionSupported() {
		logger.Fatal(errInvalidArgument,
			"Encryption support is requested but '%s' does not support encryption", name)
	}

	if strings.HasPrefix(name, "gateway") {
		if GlobalGatewaySSE.IsSet() && GlobalKMS == nil {
			uiErr := config.ErrInvalidGWSSEEnvValue(nil).Msg("MINIO_GATEWAY_SSE set but KMS is not configured")
			logger.Fatal(uiErr, "Unable to start gateway with SSE")
		}
	}

	globalCompressConfigMu.Lock()
	if globalCompressConfig.Enabled && !objAPI.IsCompressionSupported() {
		logger.Fatal(errInvalidArgument,
			"Compression support is requested but '%s' does not support compression", name)
	}
	globalCompressConfigMu.Unlock()
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

	logStartupMessage(prepareUpdateMessage("Run `mc admin update`", lrTime.Sub(crTime)))
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
		globalMinioConsolePortAuto = true
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

func handleCommonEnvVars() {
	var err error
	globalBrowserEnabled, err = config.ParseBool(env.Get(config.EnvBrowser, config.EnableOn))
	if err != nil {
		logger.Fatal(config.ErrInvalidBrowserValue(err), "Invalid MINIO_BROWSER value in environment variable")
	}

	globalBrowserRedirect, err = config.ParseBool(env.Get(config.EnvBrowserRedirect, config.EnableOn))
	if err != nil {
		logger.Fatal(config.ErrInvalidBrowserValue(err), "Invalid MINIO_BROWSER_REDIRECT value in environment variable")
	}

	globalFSOSync, err = config.ParseBool(env.Get(config.EnvFSOSync, config.EnableOff))
	if err != nil {
		logger.Fatal(config.ErrInvalidFSOSyncValue(err), "Invalid MINIO_FS_OSYNC value in environment variable")
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
		var domainIPs = set.NewStringSet()
		for _, endpoint := range minioEndpoints {
			if net.ParseIP(endpoint) == nil {
				// Checking if the IP is a DNS entry.
				addrs, err := net.LookupHost(endpoint)
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

	// Check if the supported credential env vars, "MINIO_ROOT_USER" and
	// "MINIO_ROOT_PASSWORD" are provided
	// Warn user if deprecated environment variables,
	// "MINIO_ACCESS_KEY" and "MINIO_SECRET_KEY", are defined
	// Check all error conditions first
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
	haveRootCredentials := false
	haveAccessCredentials := false
	if env.IsSet(config.EnvRootUser) && env.IsSet(config.EnvRootPassword) {
		user = env.Get(config.EnvRootUser, "")
		password = env.Get(config.EnvRootPassword, "")
		haveRootCredentials = true
	} else if env.IsSet(config.EnvAccessKey) && env.IsSet(config.EnvSecretKey) {
		user = env.Get(config.EnvAccessKey, "")
		password = env.Get(config.EnvSecretKey, "")
		haveAccessCredentials = true
	}
	if haveRootCredentials || haveAccessCredentials {
		cred, err := auth.CreateCredentials(user, password)
		if err != nil {
			logger.Fatal(config.ErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the shell environment")
		}
		if haveAccessCredentials {
			msg := fmt.Sprintf("WARNING: %s and %s are deprecated.\n"+
				"         Please use %s and %s",
				config.EnvAccessKey, config.EnvSecretKey,
				config.EnvRootUser, config.EnvRootPassword)
			logStartupMessage(color.RedBold(msg))
		}
		globalActiveCred = cred
	}

	switch {
	case env.IsSet(config.EnvKMSSecretKey) && env.IsSet(config.EnvKESEndpoint):
		logger.Fatal(errors.New("ambigious KMS configuration"), fmt.Sprintf("The environment contains %q as well as %q", config.EnvKMSSecretKey, config.EnvKESEndpoint))
	}

	if env.IsSet(config.EnvKMSSecretKey) {
		GlobalKMS, err = kms.Parse(env.Get(config.EnvKMSSecretKey, ""))
		if err != nil {
			logger.Fatal(err, "Unable to parse the KMS secret key inherited from the shell environment")
		}
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
		certificate, err := tls.LoadX509KeyPair(env.Get(config.EnvKESClientCert, ""), env.Get(config.EnvKESClientKey, ""))
		if err != nil {
			logger.Fatal(err, "Unable to load KES client certificate as specified by the shell environment")
		}
		rootCAs, err := certs.GetRootCAs(env.Get(config.EnvKESServerCA, globalCertsCADir.Get()))
		if err != nil {
			logger.Fatal(err, fmt.Sprintf("Unable to load X.509 root CAs for KES from %q", env.Get(config.EnvKESServerCA, globalCertsCADir.Get())))
		}

		var defaultKeyID = env.Get(config.EnvKESKeyName, "")
		KMS, err := kms.NewWithConfig(kms.Config{
			Endpoints:    endpoints,
			DefaultKeyID: defaultKeyID,
			Certificate:  certificate,
			RootCAs:      rootCAs,
		})
		if err != nil {
			logger.Fatal(err, "Unable to initialize a connection to KES as specified by the shell environment")
		}

		// We check that the default key ID exists or try to create it otherwise.
		// This implicitly checks that we can communicate to KES. We don't treat
		// a policy error as failure condition since MinIO may not have the permission
		// to create keys - just to generate/decrypt data encryption keys.
		if err = KMS.CreateKey(defaultKeyID); err != nil && !errors.Is(err, kes.ErrKeyExists) && !errors.Is(err, kes.ErrNotAllowed) {
			logger.Fatal(err, "Unable to initialize a connection to KES as specified by the shell environment")
		}
		GlobalKMS = KMS
	}

	if tiers := env.Get("_MINIO_DEBUG_REMOTE_TIERS_IMMEDIATELY", ""); tiers != "" {
		globalDebugRemoteTiersImmediately = strings.Split(tiers, ",")
	}
}

func logStartupMessage(msg string) {
	if globalConsoleSys != nil {
		globalConsoleSys.Send(msg, string(logger.All))
	}
	logger.StartupMessage(msg)
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
	root, err := os.Open(globalCertsDir.Get())
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
			file, err = os.Stat(filepath.Join(root.Name(), file.Name()))
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
