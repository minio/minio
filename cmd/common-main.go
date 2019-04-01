/*
 * MinIO Cloud Storage, (C) 2017, 2018 MinIO, Inc.
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
	"crypto/tls"
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	dns2 "github.com/miekg/dns"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/env"
	xnet "github.com/minio/minio/pkg/net"
)

func verifyObjectLayerFeatures(name string, objAPI ObjectLayer) {
	if (globalAutoEncryption || GlobalKMS != nil) && !objAPI.IsEncryptionSupported() {
		logger.Fatal(errInvalidArgument,
			"Encryption support is requested but '%s' does not support encryption", name)
	}

	if strings.HasPrefix(name, "gateway") {
		if GlobalGatewaySSE.IsSet() && GlobalKMS == nil {
			uiErr := uiErrInvalidGWSSEEnvValue(nil).Msg("MINIO_GATEWAY_SSE set but KMS is not configured")
			logger.Fatal(uiErr, "Unable to start gateway with SSE")
		}
	}

	if globalIsCompressionEnabled && !objAPI.IsCompressionSupported() {
		logger.Fatal(errInvalidArgument,
			"Compression support is requested but '%s' does not support compression", name)
	}
}

// Check for updates and print a notification message
func checkUpdate(mode string) {
	// Its OK to ignore any errors during doUpdate() here.
	if updateMsg, _, currentReleaseTime, latestReleaseTime, err := getUpdateInfo(2*time.Second, mode); err == nil {
		if updateMsg == "" {
			return
		}
		if globalInplaceUpdateDisabled {
			logger.StartupMessage(updateMsg)
		} else {
			logger.StartupMessage(prepareUpdateMessage("Run `mc admin update`", latestReleaseTime.Sub(currentReleaseTime)))
		}
	}
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
	globalCLIContext.Addr = ctx.GlobalString("address")
	if globalCLIContext.Addr == "" || globalCLIContext.Addr == ":"+globalMinioDefaultPort {
		globalCLIContext.Addr = ctx.String("address")
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

	// Check "compat" flag from command line argument.
	globalCLIContext.StrictS3Compat = ctx.IsSet("compat") || ctx.GlobalIsSet("compat")
}

// Env only config options
const (
	EnvDomain            = "MINIO_DOMAIN"
	EnvBrowser           = "MINIO_BROWSER"
	EnvUpdate            = "MINIO_UPDATE"
	EnvEndpoints         = "MINIO_ENDPOINTS"
	EnvEtcdEndpoints     = "MINIO_ETCD_ENDPOINTS"
	EnvEtcdClientCert    = "MINIO_ETCD_CLIENT_CERT"
	EnvEtcdClientCertKey = "MINIO_ETCD_CLIENT_CERT_KEY"
	EnvPublicIPs         = "MINIO_PUBLIC_IPS"

	EnvAccessKey = "MINIO_ACCESS_KEY"
	EnvSecretKey = "MINIO_SECRET_KEY"

	EnvInternalProfiler = "_MINIO_PROFILER"
)

func handleCommonEnvVars() {
	compressEnvDelimiter := ","
	// Start profiler if env is set.
	if profiler := env.Get(EnvInternalProfiler, ""); profiler != "" {
		var err error
		globalProfiler, err = startProfiler(profiler, "")
		logger.FatalIf(err, "Unable to setup a profiler")
	}

	accessKey := env.Get(EnvAccessKey, "")
	secretKey := env.Get(EnvSecretKey, "")
	if accessKey != "" && secretKey != "" {
		cred, err := auth.CreateCredentials(accessKey, secretKey)
		if err != nil {
			logger.Fatal(uiErrInvalidCredentials(err),
				"Unable to validate credentials inherited from the shell environment")
		}
		cred.Expiration = timeSentinel
		globalIsEnvCreds = true
		globalEnvCred = cred
	}

	var err error
	var browser string
	if browser, globalIsEnvBrowser = env.Lookup(EnvBrowser); browser != "" {
		globalIsBrowserEnabled, err = parseBool(browser)
		if err != nil {
			logger.Fatal(uiErrInvalidBrowserValue(err).Msg("Unknown value `%s`", browser),
				"Invalid MINIO_BROWSER value in environment variable")
		}
	}

	etcdEndpointsEnv, ok := env.Lookup(EnvEtcdEndpoints)
	if ok {
		etcdEndpoints := strings.Split(etcdEndpointsEnv, ",")

		var etcdSecure bool
		for _, endpoint := range etcdEndpoints {
			u, err := xnet.ParseURL(endpoint)
			if err != nil {
				logger.FatalIf(err, "Unable to initialize etcd with %s", etcdEndpoints)
			}
			// If one of the endpoint is https, we will use https directly.
			etcdSecure = etcdSecure || u.Scheme == "https"
		}

		if etcdSecure {
			// This is only to support client side certificate authentication
			// https://coreos.com/etcd/docs/latest/op-guide/security.html
			etcdClientCertFile, ok1 := env.Lookup(EnvEtcdClientCert)
			etcdClientCertKey, ok2 := env.Lookup(EnvEtcdClientCertKey)
			var getClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error)
			if ok1 && ok2 {
				getClientCertificate = func(unused *tls.CertificateRequestInfo) (*tls.Certificate, error) {
					cert, terr := tls.LoadX509KeyPair(etcdClientCertFile, etcdClientCertKey)
					return &cert, terr
				}
			}

			globalEtcdClient, err = etcd.New(etcd.Config{
				Endpoints:         etcdEndpoints,
				DialTimeout:       defaultDialTimeout,
				DialKeepAliveTime: defaultDialKeepAlive,
				TLS: &tls.Config{
					RootCAs:              globalRootCAs,
					GetClientCertificate: getClientCertificate,
				},
			})
		} else {
			globalEtcdClient, err = etcd.New(etcd.Config{
				Endpoints:         etcdEndpoints,
				DialTimeout:       defaultDialTimeout,
				DialKeepAliveTime: defaultDialKeepAlive,
			})
		}
		logger.FatalIf(err, "Unable to initialize etcd with %s", etcdEndpoints)
	}

	if v, ok := env.Lookup(EnvDomain); ok {
		for _, domainName := range strings.Split(v, ",") {
			if _, ok = dns2.IsDomainName(domainName); !ok {
				logger.Fatal(uiErrInvalidDomainValue(nil).Msg("Unknown value `%s`", domainName),
					"Invalid MINIO_DOMAIN value in environment variable")
			}
			globalDomainNames = append(globalDomainNames, domainName)
		}
	}

	if minioEndpointsEnv, ok := env.Lookup(EnvPublicIPs); ok {
		minioEndpoints := strings.Split(minioEndpointsEnv, ",")
		var domainIPs = set.NewStringSet()
		for _, endpoint := range minioEndpoints {
			if net.ParseIP(endpoint) == nil {
				// Checking if the IP is a DNS entry.
				addrs, err := net.LookupHost(endpoint)
				if err != nil {
					logger.FatalIf(err,
						"Unable to initialize MinIO server with [%s] invalid entry found in MINIO_PUBLIC_IPS", endpoint)
				}
				for _, addr := range addrs {
					domainIPs.Add(addr)
				}
				continue
			}
			domainIPs.Add(endpoint)
		}
		updateDomainIPs(domainIPs)
	} else {
		// Add found interfaces IP address to global domain IPS,
		// loopback addresses will be naturally dropped.
		updateDomainIPs(localIP4)
	}

	if len(globalDomainNames) != 0 && !globalDomainIPs.IsEmpty() && globalEtcdClient != nil {
		globalDNSConfig, err = dns.NewCoreDNS(globalDomainNames, globalDomainIPs, globalMinioPort, globalEtcdClient)
		logger.FatalIf(err, "Unable to initialize DNS config for %s.", globalDomainNames)
	}

	// In place update is true by default if the MINIO_UPDATE is not set
	// or is not set to 'off', if MINIO_UPDATE is set to 'off' then
	// in-place update is off.
	globalInplaceUpdateDisabled = strings.EqualFold(env.Get(EnvUpdate, "on"), "off")

	// Validate and store the storage class env variables only for XL/Dist XL setups
	if globalIsXL {
		// Check for environment variables and parse into storageClass struct
		if ssc := env.Get(EnvStandardStorageClass, ""); ssc != "" {
			globalStandardStorageClass, err = parseStorageClass(ssc)
			logger.FatalIf(err, "Invalid value set in environment variable %s", EnvStandardStorageClass)
		}

		if rrsc := env.Get(EnvReducedRedundancyStorageClass, ""); rrsc != "" {
			globalRRStorageClass, err = parseStorageClass(rrsc)
			logger.FatalIf(err, "Invalid value set in environment variable %s", EnvReducedRedundancyStorageClass)
		}

		// Validation is done after parsing both the storage classes. This is needed because we need one
		// storage class value to deduce the correct value of the other storage class.
		if globalRRStorageClass.Scheme != "" {
			err = validateParity(globalStandardStorageClass.Parity, globalRRStorageClass.Parity)
			logger.FatalIf(err, "Invalid value set in environment variable %s", EnvReducedRedundancyStorageClass)
			globalIsStorageClass = true
		}

		if globalStandardStorageClass.Scheme != "" {
			err = validateParity(globalStandardStorageClass.Parity, globalRRStorageClass.Parity)
			logger.FatalIf(err, "Invalid value set in environment variable %s", EnvStandardStorageClass)
			globalIsStorageClass = true
		}
	}

	// Get WORM environment variable.
	var worm string
	if worm, globalIsEnvWORM = env.Lookup(EnvWORM); globalIsEnvWORM {
		globalWORMEnabled, err = parseBool(worm)
		if err != nil {
			logger.Fatal(uiErrInvalidWormValue(err).Msg("Unknown value `%s`", worm),
				"Invalid "+EnvWORM+" value in environment variable")
		}
	}

	var compress string
	if compress, globalIsEnvCompression = env.Lookup(EnvCompress); globalIsEnvCompression {
		globalIsCompressionEnabled, err = parseBool(compress)
		if err != nil {
			logger.Fatal(err, "Invalid "+EnvCompress+" value in environment variable")
		}
	}

	if globalIsEnvCompression {
		compressExtensions := env.Get(EnvCompressExtensions, "")
		compressMimeTypes := env.Get(EnvCompressMimeTypes, "")
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

	if drives := env.Get(EnvCacheDrives, ""); drives != "" {
		driveList, err := parseCacheDrives(strings.Split(drives, cacheEnvDelimiter))
		if err != nil {
			logger.Fatal(err, "Unable to parse MINIO_CACHE_DRIVES value (%s)", drives)
		}
		globalCacheDrives = driveList
		globalIsDiskCacheEnabled = true
	}

	if globalIsDiskCacheEnabled {
		if excludes := env.Get(EnvCacheExclude, ""); excludes != "" {
			excludeList, err := parseCacheExcludes(strings.Split(excludes, cacheEnvDelimiter))
			if err != nil {
				logger.Fatal(err, "Unable to parse MINIO_CACHE_EXCLUDE value (`%s`)", excludes)
			}
			globalCacheExcludes = excludeList
		}

		if expiryStr := env.Get(EnvCacheExpiry, ""); expiryStr != "" {
			expiry, err := strconv.Atoi(expiryStr)
			if err != nil {
				logger.Fatal(uiErrInvalidCacheExpiryValue(err),
					"Unable to parse MINIO_CACHE_EXPIRY value (`%s`)", expiryStr)
			}
			globalCacheExpiry = expiry
		}

		if quotaStr := env.Get(EnvCacheQuota, ""); quotaStr != "" {
			quota, err := strconv.Atoi(quotaStr)
			if err != nil {
				logger.Fatal(uiErrInvalidCacheMaxUse(err),
					"Unable to parse MINIO_CACHE_MAXUSE value (`%s`)", quotaStr)
			}
			// quota should be a valid percentage.
			if quota > 0 && quota <= 100 {
				globalCacheQuota = quota
			}
		}

		if cacheEncKey := env.Get(EnvCacheEncryptionMasterKey, ""); cacheEncKey != "" {
			cacheKMSKeyID, cacheKMS, err := crypto.ParseKMSMasterKey(cacheEncKey)
			if err != nil {
				logger.Fatal(uiErrInvalidCacheEncryptionKey(err), "Invalid cache encryption master key")
			}
			globalCacheKMSKeyID, globalCacheKMS = cacheKMSKeyID, cacheKMS
		}
	}
}
