/*
 * Minio Cloud Storage, (C) 2017, 2018 Minio, Inc.
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
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	dns2 "github.com/miekg/dns"
	"github.com/minio/cli"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/dns"
)

// Check for updates and print a notification message
func checkUpdate(mode string) {
	// Its OK to ignore any errors during doUpdate() here.
	if updateMsg, _, currentReleaseTime, latestReleaseTime, err := getUpdateInfo(2*time.Second, mode); err == nil {
		if globalInplaceUpdateDisabled {
			logger.StartupMessage(updateMsg)
		} else {
			logger.StartupMessage(prepareUpdateMessage("Run `minio update`", latestReleaseTime.Sub(currentReleaseTime)))
		}
	}
}

// Load logger targets based on user's configuration
func loadLoggers() {
	if endpoint, ok := os.LookupEnv("MINIO_LOGGER_HTTP_ENDPOINT"); ok {
		// Enable http logging through ENV, this is specifically added gateway audit logging.
		logger.AddTarget(logger.NewHTTP(endpoint, NewCustomHTTPTransport()))
		return
	}

	if globalServerConfig.Logger.Console.Enabled {
		// Enable console logging
		logger.AddTarget(logger.NewConsole())
	}

	for _, l := range globalServerConfig.Logger.HTTP {
		if l.Enabled {
			// Enable http logging
			logger.AddTarget(logger.NewHTTP(l.Endpoint, NewCustomHTTPTransport()))
		}
	}
}

func handleCommonCmdArgs(ctx *cli.Context) {

	var configDir string

	switch {
	case ctx.IsSet("config-dir"):
		configDir = ctx.String("config-dir")
	case ctx.GlobalIsSet("config-dir"):
		configDir = ctx.GlobalString("config-dir")
		// cli package does not expose parent's "config-dir" option.  Below code is workaround.
		if configDir == "" || configDir == getConfigDir() {
			if ctx.Parent().GlobalIsSet("config-dir") {
				configDir = ctx.Parent().GlobalString("config-dir")
			}
		}
	default:
		// Neither local nor global config-dir option is provided.  In this case, try to use
		// default config directory.
		configDir = getConfigDir()
		if configDir == "" {
			logger.FatalIf(errors.New("missing option"), "config-dir option must be provided")
		}
	}

	if configDir == "" {
		logger.FatalIf(errors.New("empty directory"), "Configuration directory cannot be empty")
	}

	// Disallow relative paths, figure out absolute paths.
	configDirAbs, err := filepath.Abs(configDir)
	logger.FatalIf(err, "Unable to fetch absolute path for config directory %s", configDir)
	setConfigDir(configDirAbs)
}

// Parses the given compression exclude list `extensions` or `content-types`.
func parseCompressIncludes(includes []string) ([]string, error) {
	for _, e := range includes {
		if len(e) == 0 {
			return nil, uiErrInvalidCompressionIncludesValue(nil).Msg("extension/mime-type (%s) cannot be empty", e)
		}
	}
	return includes, nil
}

func handleCommonEnvVars() {
	compressEnvDelimiter := ","
	// Start profiler if env is set.
	if profiler := os.Getenv("_MINIO_PROFILER"); profiler != "" {
		var err error
		globalProfiler, err = startProfiler(profiler, "")
		logger.FatalIf(err, "Unable to setup a profiler")
	}

	accessKey := os.Getenv("MINIO_ACCESS_KEY")
	secretKey := os.Getenv("MINIO_SECRET_KEY")
	if accessKey != "" && secretKey != "" {
		cred, err := auth.CreateCredentials(accessKey, secretKey)
		if err != nil {
			logger.Fatal(uiErrInvalidCredentials(err), "Unable to validate credentials inherited from the shell environment")
		}
		cred.Expiration = timeSentinel

		// credential Envs are set globally.
		globalIsEnvCreds = true
		globalActiveCred = cred
	}

	if browser := os.Getenv("MINIO_BROWSER"); browser != "" {
		browserFlag, err := ParseBoolFlag(browser)
		if err != nil {
			logger.Fatal(uiErrInvalidBrowserValue(nil).Msg("Unknown value `%s`", browser), "Invalid MINIO_BROWSER value in environment variable")
		}

		// browser Envs are set globally, this does not represent
		// if browser is turned off or on.
		globalIsEnvBrowser = true
		globalIsBrowserEnabled = bool(browserFlag)
	}

	traceFile := os.Getenv("MINIO_HTTP_TRACE")
	if traceFile != "" {
		var err error
		globalHTTPTraceFile, err = os.OpenFile(traceFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
		logger.FatalIf(err, "error opening file %s", traceFile)
	}

	etcdEndpointsEnv, ok := os.LookupEnv("MINIO_ETCD_ENDPOINTS")
	if ok {
		etcdEndpoints := strings.Split(etcdEndpointsEnv, ",")
		var err error
		globalEtcdClient, err = etcd.New(etcd.Config{
			Endpoints:         etcdEndpoints,
			DialTimeout:       defaultDialTimeout,
			DialKeepAliveTime: defaultDialKeepAlive,
		})
		logger.FatalIf(err, "Unable to initialize etcd with %s", etcdEndpoints)
	}

	globalDomainName, globalIsEnvDomainName = os.LookupEnv("MINIO_DOMAIN")
	if globalDomainName != "" {
		if _, ok = dns2.IsDomainName(globalDomainName); !ok {
			logger.Fatal(uiErrInvalidDomainValue(nil).Msg("Unknown value `%s`", globalDomainName), "Invalid MINIO_DOMAIN value in environment variable")
		}
	}

	minioEndpointsEnv, ok := os.LookupEnv("MINIO_PUBLIC_IPS")
	if ok {
		minioEndpoints := strings.Split(minioEndpointsEnv, ",")
		globalDomainIPs = set.NewStringSet()
		for i, ip := range minioEndpoints {
			if net.ParseIP(ip) == nil {
				logger.FatalIf(errInvalidArgument, "Unable to initialize Minio server with invalid MINIO_PUBLIC_IPS[%d]: %s", i, ip)
			}
			globalDomainIPs.Add(ip)
		}
	}
	if globalDomainName != "" && !globalDomainIPs.IsEmpty() && globalEtcdClient != nil {
		var err error
		globalDNSConfig, err = dns.NewCoreDNS(globalDomainName, globalDomainIPs, globalMinioPort, globalEtcdClient)
		logger.FatalIf(err, "Unable to initialize DNS config for %s.", globalDomainName)
	}

	if drives := os.Getenv("MINIO_CACHE_DRIVES"); drives != "" {
		driveList, err := parseCacheDrives(strings.Split(drives, cacheEnvDelimiter))
		if err != nil {
			logger.Fatal(err, "Unable to parse MINIO_CACHE_DRIVES value (%s)", drives)
		}
		globalCacheDrives = driveList
		globalIsDiskCacheEnabled = true
	}

	if excludes := os.Getenv("MINIO_CACHE_EXCLUDE"); excludes != "" {
		excludeList, err := parseCacheExcludes(strings.Split(excludes, cacheEnvDelimiter))
		if err != nil {
			logger.Fatal(err, "Unable to parse MINIO_CACHE_EXCLUDE value (`%s`)", excludes)
		}
		globalCacheExcludes = excludeList
	}

	if expiryStr := os.Getenv("MINIO_CACHE_EXPIRY"); expiryStr != "" {
		expiry, err := strconv.Atoi(expiryStr)
		if err != nil {
			logger.Fatal(uiErrInvalidCacheExpiryValue(err), "Unable to parse MINIO_CACHE_EXPIRY value (`%s`)", expiryStr)
		}
		globalCacheExpiry = expiry
	}

	if maxUseStr := os.Getenv("MINIO_CACHE_MAXUSE"); maxUseStr != "" {
		maxUse, err := strconv.Atoi(maxUseStr)
		if err != nil {
			logger.Fatal(uiErrInvalidCacheMaxUse(err), "Unable to parse MINIO_CACHE_MAXUSE value (`%s`)", maxUseStr)
		}
		// maxUse should be a valid percentage.
		if maxUse > 0 && maxUse <= 100 {
			globalCacheMaxUse = maxUse
		}
	}
	// In place update is true by default if the MINIO_UPDATE is not set
	// or is not set to 'off', if MINIO_UPDATE is set to 'off' then
	// in-place update is off.
	globalInplaceUpdateDisabled = strings.EqualFold(os.Getenv("MINIO_UPDATE"), "off")

	// Validate and store the storage class env variables only for XL/Dist XL setups
	if globalIsXL {
		var err error

		// Check for environment variables and parse into storageClass struct
		if ssc := os.Getenv(standardStorageClassEnv); ssc != "" {
			globalStandardStorageClass, err = parseStorageClass(ssc)
			logger.FatalIf(err, "Invalid value set in environment variable %s", standardStorageClassEnv)
		}

		if rrsc := os.Getenv(reducedRedundancyStorageClassEnv); rrsc != "" {
			globalRRStorageClass, err = parseStorageClass(rrsc)
			logger.FatalIf(err, "Invalid value set in environment variable %s", reducedRedundancyStorageClassEnv)
		}

		// Validation is done after parsing both the storage classes. This is needed because we need one
		// storage class value to deduce the correct value of the other storage class.
		if globalRRStorageClass.Scheme != "" {
			err = validateParity(globalStandardStorageClass.Parity, globalRRStorageClass.Parity)
			logger.FatalIf(err, "Invalid value set in environment variable %s", reducedRedundancyStorageClassEnv)
			globalIsStorageClass = true
		}

		if globalStandardStorageClass.Scheme != "" {
			err = validateParity(globalStandardStorageClass.Parity, globalRRStorageClass.Parity)
			logger.FatalIf(err, "Invalid value set in environment variable %s", standardStorageClassEnv)
			globalIsStorageClass = true
		}
	}

	// Get WORM environment variable.
	if worm := os.Getenv("MINIO_WORM"); worm != "" {
		wormFlag, err := ParseBoolFlag(worm)
		if err != nil {
			logger.Fatal(uiErrInvalidWormValue(nil).Msg("Unknown value `%s`", worm), "Invalid MINIO_WORM value in environment variable")
		}

		// worm Envs are set globally, this does not represent
		// if worm is turned off or on.
		globalIsEnvWORM = true
		globalWORMEnabled = bool(wormFlag)
	}

	kmsConf, err := crypto.NewVaultConfig()
	if err != nil {
		logger.Fatal(err, "Unable to initialize hashicorp vault")
	}
	if kmsConf.Vault.Endpoint != "" {
		kms, err := crypto.NewVault(kmsConf)
		if err != nil {
			logger.Fatal(err, "Unable to initialize KMS")
		}
		globalKMS = kms
		globalKMSKeyID = kmsConf.Vault.Key.Name
		globalKMSConfig = kmsConf
	}

	if compress := os.Getenv("MINIO_COMPRESS"); compress != "" {
		globalIsCompressionEnabled = strings.EqualFold(compress, "true")
	}

	compressExtensions := os.Getenv("MINIO_COMPRESS_EXTENSIONS")
	compressMimeTypes := os.Getenv("MINIO_COMPRESS_MIMETYPES")
	if compressExtensions != "" || compressMimeTypes != "" {
		globalIsEnvCompression = true
		if compressExtensions != "" {
			extensions, err := parseCompressIncludes(strings.Split(compressExtensions, compressEnvDelimiter))
			if err != nil {
				logger.Fatal(err, "Invalid MINIO_COMPRESS_EXTENSIONS value (`%s`)", extensions)
			}
			globalCompressExtensions = extensions
		}
		if compressMimeTypes != "" {
			contenttypes, err := parseCompressIncludes(strings.Split(compressMimeTypes, compressEnvDelimiter))
			if err != nil {
				logger.Fatal(err, "Invalid MINIO_COMPRESS_MIMETYPES value (`%s`)", contenttypes)
			}
			globalCompressMimeTypes = contenttypes
		}
	}
}
