// Copyright (c) 2015-2023 MinIO, Inc.
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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/minio/cli"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v2/certs"
	"github.com/minio/pkg/v2/env"
	"golang.org/x/exp/slices"
)

// ServerFlags - server command specific flags
var ServerFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "address",
		Value:  ":" + GlobalMinioDefaultPort,
		Usage:  "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
		EnvVar: "MINIO_ADDRESS",
	},
	cli.IntFlag{
		Name:   "listeners", // Deprecated Oct 2022
		Value:  1,
		Usage:  "bind N number of listeners per ADDRESS:PORT",
		EnvVar: "MINIO_LISTENERS",
		Hidden: true,
	},
	cli.StringFlag{
		Name:   "console-address",
		Usage:  "bind to a specific ADDRESS:PORT for embedded Console UI, ADDRESS can be an IP or hostname",
		EnvVar: "MINIO_CONSOLE_ADDRESS",
	},
	cli.DurationFlag{
		Name:   "shutdown-timeout",
		Value:  xhttp.DefaultShutdownTimeout,
		Usage:  "shutdown timeout to gracefully shutdown server",
		EnvVar: "MINIO_SHUTDOWN_TIMEOUT",
		Hidden: true,
	},
	cli.DurationFlag{
		Name:   "idle-timeout",
		Value:  xhttp.DefaultIdleTimeout,
		Usage:  "idle timeout is the maximum amount of time to wait for the next request when keep-alives are enabled",
		EnvVar: "MINIO_IDLE_TIMEOUT",
		Hidden: true,
	},
	cli.DurationFlag{
		Name:   "read-header-timeout",
		Value:  xhttp.DefaultReadHeaderTimeout,
		Usage:  "read header timeout is the amount of time allowed to read request headers",
		EnvVar: "MINIO_READ_HEADER_TIMEOUT",
		Hidden: true,
	},
	cli.DurationFlag{
		Name:   "conn-read-deadline",
		Usage:  "custom connection READ deadline",
		Hidden: true,
		Value:  10 * time.Minute,
		EnvVar: "MINIO_CONN_READ_DEADLINE",
	},
	cli.DurationFlag{
		Name:   "conn-write-deadline",
		Usage:  "custom connection WRITE deadline",
		Hidden: true,
		Value:  10 * time.Minute,
		EnvVar: "MINIO_CONN_WRITE_DEADLINE",
	},
	cli.DurationFlag{
		Name:   "conn-user-timeout",
		Usage:  "custom TCP_USER_TIMEOUT for socket buffers",
		Hidden: true,
		Value:  10 * time.Minute,
		EnvVar: "MINIO_CONN_USER_TIMEOUT",
	},
	cli.StringFlag{
		Name:   "interface",
		Usage:  "bind to right VRF device for MinIO services",
		Hidden: true,
		EnvVar: "MINIO_INTERFACE",
	},
	cli.DurationFlag{
		Name:   "dns-cache-ttl",
		Usage:  "custom DNS cache TTL for baremetal setups",
		Hidden: true,
		Value:  10 * time.Minute,
		EnvVar: "MINIO_DNS_CACHE_TTL",
	},
	cli.StringSliceFlag{
		Name:  "ftp",
		Usage: "enable and configure an FTP(Secure) server",
	},
	cli.StringSliceFlag{
		Name:  "sftp",
		Usage: "enable and configure an SFTP server",
	},
}

var gatewayCmd = cli.Command{
	Name:            "gateway",
	Usage:           "start object storage gateway",
	Hidden:          true,
	Flags:           append(ServerFlags, GlobalFlags...),
	HideHelpCommand: true,
	Action:          gatewayMain,
}

func gatewayMain(ctx *cli.Context) error {
	logger.Fatal(errInvalidArgument, "Gateway is deprecated, To continue to use Gateway please use releases no later than 'RELEASE.2022-10-24T18-35-07Z'. We recommend all our users to migrate from gateway mode to server mode. Please read https://blog.min.io/deprecation-of-the-minio-gateway/")
	return nil
}

var serverCmd = cli.Command{
	Name:   "server",
	Usage:  "start object storage server",
	Flags:  append(ServerFlags, GlobalFlags...),
	Action: serverMain,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR1 [DIR2..]
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64}
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS] {{end}}DIR{1...64} DIR{65...128}

DIR:
  DIR points to a directory on a filesystem. When you want to combine
  multiple drives into a single large system, pass one directory per
  filesystem separated by space. You may also use a '...' convention
  to abbreviate the directory arguments. Remote directories in a
  distributed setup are encoded as HTTP(s) URIs.
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
  1. Start MinIO server on "/home/shared" directory.
     {{.Prompt}} {{.HelpName}} /home/shared

  2. Start single node server with 64 local drives "/mnt/data1" to "/mnt/data64".
     {{.Prompt}} {{.HelpName}} /mnt/data{1...64}

  3. Start distributed MinIO server on an 32 node setup with 32 drives each, run following command on all the nodes
     {{.Prompt}} {{.HelpName}} http://node{1...32}.example.com/mnt/export{1...32}

  4. Start distributed MinIO server in an expanded setup, run the following command on all the nodes
     {{.Prompt}} {{.HelpName}} http://node{1...16}.example.com/mnt/export{1...32} \
            http://node{17...64}.example.com/mnt/export{1...64}

  5. Start distributed MinIO server, with FTP and SFTP servers on all interfaces via port 8021, 8022 respectively
     {{.Prompt}} {{.HelpName}} http://node{1...4}.example.com/mnt/export{1...4} \
           --ftp="address=:8021" --ftp="passive-port-range=30000-40000" \
           --sftp="address=:8022" --sftp="ssh-private-key=${HOME}/.ssh/id_rsa"
`,
}

func serverCmdArgs(ctx *cli.Context) []string {
	v, _, _, err := env.LookupEnv(config.EnvArgs)
	if err != nil {
		logger.FatalIf(err, "Unable to validate passed arguments in %s:%s",
			config.EnvArgs, os.Getenv(config.EnvArgs))
	}
	if v == "" {
		v, _, _, err = env.LookupEnv(config.EnvVolumes)
		if err != nil {
			logger.FatalIf(err, "Unable to validate passed arguments in %s:%s",
				config.EnvVolumes, os.Getenv(config.EnvVolumes))
		}
	}
	if v == "" {
		// Fall back to older environment value MINIO_ENDPOINTS
		v, _, _, err = env.LookupEnv(config.EnvEndpoints)
		if err != nil {
			logger.FatalIf(err, "Unable to validate passed arguments in %s:%s",
				config.EnvEndpoints, os.Getenv(config.EnvEndpoints))
		}
	}
	if v == "" {
		if !ctx.Args().Present() || ctx.Args().First() == "help" {
			cli.ShowCommandHelpAndExit(ctx, ctx.Command.Name, 1)
		}
		return ctx.Args()
	}
	return strings.Fields(v)
}

func serverHandleCmdArgs(ctx *cli.Context) {
	// Handle common command args.
	handleCommonCmdArgs(ctx)

	logger.FatalIf(CheckLocalServerAddr(globalMinioAddr), "Unable to validate passed arguments")

	var err error
	var setupType SetupType

	// Check and load TLS certificates.
	globalPublicCerts, globalTLSCerts, globalIsTLS, err = getTLSConfig()
	logger.FatalIf(err, "Unable to load the TLS configuration")

	// Check and load Root CAs.
	globalRootCAs, err = certs.GetRootCAs(globalCertsCADir.Get())
	logger.FatalIf(err, "Failed to read root CAs (%v)", err)

	// Add the global public crts as part of global root CAs
	for _, publicCrt := range globalPublicCerts {
		globalRootCAs.AddCert(publicCrt)
	}

	// Register root CAs for remote ENVs
	env.RegisterGlobalCAs(globalRootCAs)

	globalEndpoints, setupType, err = createServerEndpoints(globalMinioAddr, serverCmdArgs(ctx)...)
	logger.FatalIf(err, "Invalid command line arguments")
	globalNodes = globalEndpoints.GetNodes()

	globalLocalNodeName = GetLocalPeer(globalEndpoints, globalMinioHost, globalMinioPort)
	nodeNameSum := sha256.Sum256([]byte(globalLocalNodeName))
	globalLocalNodeNameHex = hex.EncodeToString(nodeNameSum[:])

	// Initialize, see which NIC the service is running on, and save it as global value
	setGlobalInternodeInterface(ctx.String("interface"))

	// allow transport to be HTTP/1.1 for proxying.
	globalProxyTransport = NewCustomHTTPProxyTransport()()
	globalProxyEndpoints = GetProxyEndpoints(globalEndpoints)
	globalInternodeTransport = NewInternodeHTTPTransport()()
	globalRemoteTargetTransport = NewRemoteTargetHTTPTransport(false)()

	globalForwarder = handlers.NewForwarder(&handlers.Forwarder{
		PassHost:     true,
		RoundTripper: NewHTTPTransportWithTimeout(1 * time.Hour),
		Logger: func(err error) {
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.LogIf(GlobalContext, err)
			}
		},
	})

	globalTCPOptions = xhttp.TCPOptions{
		UserTimeout: int(ctx.Duration("conn-user-timeout").Milliseconds()),
		Interface:   ctx.String("interface"),
	}

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	logger.FatalIf(xhttp.CheckPortAvailability(globalMinioHost, globalMinioPort, globalTCPOptions), "Unable to start the server")

	globalIsErasure = (setupType == ErasureSetupType)
	globalIsDistErasure = (setupType == DistErasureSetupType)
	if globalIsDistErasure {
		globalIsErasure = true
	}
	globalIsErasureSD = (setupType == ErasureSDSetupType)

	globalConnReadDeadline = ctx.Duration("conn-read-deadline")
	globalConnWriteDeadline = ctx.Duration("conn-write-deadline")
}

func serverHandleEnvVars() {
	// Handle common environment variables.
	handleCommonEnvVars()
}

var globalHealStateLK sync.RWMutex

func initAllSubsystems(ctx context.Context) {
	globalHealStateLK.Lock()
	// New global heal state
	globalAllHealState = newHealState(ctx, true)
	globalBackgroundHealState = newHealState(ctx, false)
	globalHealStateLK.Unlock()

	// Initialize notification peer targets
	globalNotificationSys = NewNotificationSys(globalEndpoints)

	// Create new notification system
	globalEventNotifier = NewEventNotifier()

	// Create new bucket metadata system.
	if globalBucketMetadataSys == nil {
		globalBucketMetadataSys = NewBucketMetadataSys()
	} else {
		// Reinitialize safely when testing.
		globalBucketMetadataSys.Reset()
	}

	// Create the bucket bandwidth monitor
	globalBucketMonitor = bandwidth.NewMonitor(ctx, totalNodeCount())

	// Create a new config system.
	globalConfigSys = NewConfigSys()

	// Create new IAM system.
	globalIAMSys = NewIAMSys()

	// Create new policy system.
	globalPolicySys = NewPolicySys()

	// Create new lifecycle system.
	globalLifecycleSys = NewLifecycleSys()

	// Create new bucket encryption subsystem
	globalBucketSSEConfigSys = NewBucketSSEConfigSys()

	// Create new bucket object lock subsystem
	globalBucketObjectLockSys = NewBucketObjectLockSys()

	// Create new bucket quota subsystem
	globalBucketQuotaSys = NewBucketQuotaSys()

	// Create new bucket versioning subsystem
	if globalBucketVersioningSys == nil {
		globalBucketVersioningSys = NewBucketVersioningSys()
	}

	// Create new bucket replication subsytem
	globalBucketTargetSys = NewBucketTargetSys(GlobalContext)

	// Create new ILM tier configuration subsystem
	globalTierConfigMgr = NewTierConfigMgr()
	globalTierJournal = NewTierJournal()

	globalTransitionState = newTransitionState(GlobalContext)
	globalSiteResyncMetrics = newSiteResyncMetrics(GlobalContext)
}

func configRetriableErrors(err error) bool {
	// Initializing sub-systems needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	//  - Write quorum not met when upgrading configuration
	//    version is needed, migration is needed etc.
	rquorum := InsufficientReadQuorum{}
	wquorum := InsufficientWriteQuorum{}

	// One of these retriable errors shall be retried.
	return errors.Is(err, errDiskNotFound) ||
		errors.Is(err, errConfigNotFound) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errErasureWriteQuorum) ||
		errors.Is(err, errErasureReadQuorum) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.As(err, &rquorum) ||
		errors.As(err, &wquorum) ||
		isErrObjectNotFound(err) ||
		isErrBucketNotFound(err) ||
		errors.Is(err, os.ErrDeadlineExceeded)
}

func bootstrapTraceMsg(msg string) {
	info := madmin.TraceInfo{
		TraceType: madmin.TraceBootstrap,
		Time:      UTCNow(),
		NodeName:  globalLocalNodeName,
		FuncName:  "BOOTSTRAP",
		Message:   fmt.Sprintf("%s %s", getSource(2), msg),
	}
	globalBootstrapTracer.Record(info)

	if serverDebugLog {
		logger.Info(fmt.Sprint(time.Now().Round(time.Millisecond).Format(time.RFC3339), " bootstrap: ", msg))
	}

	noSubs := globalTrace.NumSubscribers(madmin.TraceBootstrap) == 0
	if noSubs {
		return
	}

	globalTrace.Publish(info)
}

func bootstrapTrace(msg string, worker func()) {
	if serverDebugLog {
		logger.Info(fmt.Sprint(time.Now().Round(time.Millisecond).Format(time.RFC3339), " bootstrap: ", msg))
	}

	now := time.Now()
	worker()
	dur := time.Since(now)

	info := madmin.TraceInfo{
		TraceType: madmin.TraceBootstrap,
		Time:      UTCNow(),
		NodeName:  globalLocalNodeName,
		FuncName:  "BOOTSTRAP",
		Message:   fmt.Sprintf("%s %s (duration: %s)", getSource(2), msg, dur),
	}
	globalBootstrapTracer.Record(info)

	if globalTrace.NumSubscribers(madmin.TraceBootstrap) == 0 {
		return
	}

	globalTrace.Publish(info)
}

func initServerConfig(ctx context.Context, newObject ObjectLayer) error {
	t1 := time.Now()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			// Retry was canceled successfully.
			return fmt.Errorf("Initializing sub-systems stopped gracefully %w", ctx.Err())
		default:
		}

		// These messages only meant primarily for distributed setup, so only log during distributed setup.
		if globalIsDistErasure {
			logger.Info("Waiting for all MinIO sub-systems to be initialize...")
		}

		// Upon success migrating the config, initialize all sub-systems
		// if all sub-systems initialized successfully return right away
		err := initConfigSubsystem(ctx, newObject)
		if err == nil {
			// All successful return.
			if globalIsDistErasure {
				// These messages only meant primarily for distributed setup, so only log during distributed setup.
				logger.Info("All MinIO sub-systems initialized successfully in %s", time.Since(t1))
			}
			return nil
		}

		if configRetriableErrors(err) {
			logger.Info("Waiting for all MinIO sub-systems to be initialized.. possible cause (%v)", err)
			time.Sleep(time.Duration(r.Float64() * float64(5*time.Second)))
			continue
		}

		// Any other unhandled return right here.
		return fmt.Errorf("Unable to initialize sub-systems: %w", err)
	}
}

func initConfigSubsystem(ctx context.Context, newObject ObjectLayer) error {
	// %w is used by all error returns here to make sure
	// we wrap the underlying error, make sure when you
	// are modifying this code that you do so, if and when
	// you want to add extra context to your error. This
	// ensures top level retry works accordingly.

	// Initialize config system.
	if err := globalConfigSys.Init(newObject); err != nil {
		if configRetriableErrors(err) {
			return fmt.Errorf("Unable to initialize config system: %w", err)
		}

		// Any other config errors we simply print a message and proceed forward.
		logger.LogIf(ctx, fmt.Errorf("Unable to initialize config, some features may be missing: %w", err))
	}

	return nil
}

func setGlobalInternodeInterface(interfaceName string) {
	globalInternodeInterfaceOnce.Do(func() {
		if interfaceName != "" {
			globalInternodeInterface = interfaceName
			return
		}
		ip := "127.0.0.1"
		host, _ := mustSplitHostPort(globalLocalNodeName)
		if host != "" {
			if net.ParseIP(host) != nil {
				ip = host
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				haddrs, err := globalDNSCache.LookupHost(ctx, host)
				if err == nil {
					ip = haddrs[0]
				}
			}
		}
		ifs, _ := net.Interfaces()
		for _, interf := range ifs {
			addrs, err := interf.Addrs()
			if err == nil {
				for _, addr := range addrs {
					if strings.SplitN(addr.String(), "/", 2)[0] == ip {
						globalInternodeInterface = interf.Name
					}
				}
			}
		}
	})
}

// Return the list of address that MinIO server needs to listen on:
//   - Returning 127.0.0.1 is necessary so Console will be able to send
//     requests to the local S3 API.
//   - The returned List needs to be deduplicated as well.
func getServerListenAddrs() []string {
	// Use a string set to avoid duplication
	addrs := set.NewStringSet()
	// Listen on local interface to receive requests from Console
	for _, ip := range mustGetLocalIPs() {
		if ip != nil && ip.IsLoopback() {
			addrs.Add(net.JoinHostPort(ip.String(), globalMinioPort))
		}
	}
	host, _ := mustSplitHostPort(globalMinioAddr)
	if host != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		haddrs, err := globalDNSCache.LookupHost(ctx, host)
		if err == nil {
			for _, addr := range haddrs {
				addrs.Add(net.JoinHostPort(addr, globalMinioPort))
			}
		} else {
			// Unable to lookup host in 2-secs, let it fail later anyways.
			addrs.Add(globalMinioAddr)
		}
	} else {
		addrs.Add(globalMinioAddr)
	}
	return addrs.ToSlice()
}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go handleSignals()

	setDefaultProfilerRates()

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Handle all server command args.
	bootstrapTrace("serverHandleCmdArgs", func() {
		serverHandleCmdArgs(ctx)
	})

	// Initialize globalConsoleSys system
	bootstrapTrace("newConsoleLogger", func() {
		globalConsoleSys = NewConsoleLogger(GlobalContext)
		logger.AddSystemTarget(GlobalContext, globalConsoleSys)

		// Set node name, only set for distributed setup.
		globalConsoleSys.SetNodeName(globalLocalNodeName)
	})

	// Perform any self-tests
	bootstrapTrace("selftests", func() {
		bitrotSelfTest()
		erasureSelfTest()
		compressSelfTest()
	})

	// Initialize KMS configuration
	bootstrapTrace("handleKMSConfig", handleKMSConfig)

	// Initialize all help
	bootstrapTrace("initHelp", initHelp)

	// Initialize all sub-systems
	bootstrapTrace("initAllSubsystems", func() {
		initAllSubsystems(GlobalContext)
	})

	// Is distributed setup, error out if no certificates are found for HTTPS endpoints.
	if globalIsDistErasure {
		if globalEndpoints.HTTPS() && !globalIsTLS {
			logger.Fatal(config.ErrNoCertsAndHTTPSEndpoints(nil), "Unable to start the server")
		}
		if !globalEndpoints.HTTPS() && globalIsTLS {
			logger.Fatal(config.ErrCertsAndHTTPEndpoints(nil), "Unable to start the server")
		}
	}

	// Check for updates in non-blocking manner.
	go func() {
		if !globalCLIContext.Quiet && !globalInplaceUpdateDisabled {
			// Check for new updates from dl.min.io.
			bootstrapTrace("checkUpdate", func() {
				checkUpdate(getMinioMode())
			})
		}
	}()

	// Set system resources to maximum.
	bootstrapTrace("setMaxResources", func() {
		_ = setMaxResources()
	})

	// Verify kernel release and version.
	if oldLinux() {
		logger.Info(color.RedBold("WARNING: Detected Linux kernel version older than 4.0.0 release, there are some known potential performance problems with this kernel version. MinIO recommends a minimum of 4.x.x linux kernel version for best performance"))
	}

	maxProcs := runtime.GOMAXPROCS(0)
	cpuProcs := runtime.NumCPU()
	if maxProcs < cpuProcs {
		logger.Info(color.RedBoldf("WARNING: Detected GOMAXPROCS(%d) < NumCPU(%d), please make sure to provide all PROCS to MinIO for optimal performance", maxProcs, cpuProcs))
	}

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	// Configure server.
	bootstrapTrace("configureServer", func() {
		handler, err := configureServerHandler(globalEndpoints)
		if err != nil {
			logger.Fatal(config.ErrUnexpectedError(err), "Unable to configure one of server's RPC services")
		}

		httpServer := xhttp.NewServer(getServerListenAddrs()).
			UseHandler(setCriticalErrorHandler(corsHandler(handler))).
			UseTLSConfig(newTLSConfig(getCert)).
			UseShutdownTimeout(ctx.Duration("shutdown-timeout")).
			UseIdleTimeout(ctx.Duration("idle-timeout")).
			UseReadHeaderTimeout(ctx.Duration("read-header-timeout")).
			UseBaseContext(GlobalContext).
			UseCustomLogger(log.New(io.Discard, "", 0)). // Turn-off random logging by Go stdlib
			UseTCPOptions(globalTCPOptions)

		httpServer.TCPOptions.Trace = bootstrapTraceMsg
		go func() {
			serveFn, err := httpServer.Init(GlobalContext, func(listenAddr string, err error) {
				logger.LogIf(GlobalContext, fmt.Errorf("Unable to listen on `%s`: %v", listenAddr, err))
			})
			if err != nil {
				globalHTTPServerErrorCh <- err
				return
			}
			globalHTTPServerErrorCh <- serveFn()
		}()

		setHTTPServer(httpServer)
	})

	if globalIsDistErasure {
		bootstrapTrace("verifying system configuration", func() {
			// Additionally in distributed setup, validate the setup and configuration.
			if err := verifyServerSystemConfig(GlobalContext, globalEndpoints); err != nil {
				logger.Fatal(err, "Unable to start the server")
			}
		})
	}

	if !globalDisableFreezeOnBoot {
		// Freeze the services until the bucket notification subsystem gets initialized.
		bootstrapTrace("freezeServices", freezeServices)
	}

	var newObject ObjectLayer
	bootstrapTrace("newObjectLayer", func() {
		var err error
		newObject, err = newObjectLayer(GlobalContext, globalEndpoints)
		if err != nil {
			logFatalErrs(err, Endpoint{}, true)
		}
	})

	xhttp.SetDeploymentID(globalDeploymentID)
	xhttp.SetMinIOVersion(Version)

	for _, n := range globalNodes {
		nodeName := n.Host
		if n.IsLocal {
			nodeName = globalLocalNodeName
		}
		nodeNameSum := sha256.Sum256([]byte(nodeName + globalDeploymentID))
		globalNodeNamesHex[hex.EncodeToString(nodeNameSum[:])] = struct{}{}
	}

	bootstrapTrace("newSharedLock", func() {
		globalLeaderLock = newSharedLock(GlobalContext, newObject, "leader.lock")
	})

	// Enable background operations on
	//
	// - Disk auto healing
	// - MRF (most recently failed) healing
	// - Background expiration routine for lifecycle policies
	bootstrapTrace("initAutoHeal", func() {
		initAutoHeal(GlobalContext, newObject)
	})

	bootstrapTrace("initHealMRF", func() {
		initHealMRF(GlobalContext, newObject)
	})

	bootstrapTrace("initBackgroundExpiry", func() {
		initBackgroundExpiry(GlobalContext, newObject)
	})

	var err error
	bootstrapTrace("initServerConfig", func() {
		if err = initServerConfig(GlobalContext, newObject); err != nil {
			var cerr config.Err
			// For any config error, we don't need to drop into safe-mode
			// instead its a user error and should be fixed by user.
			if errors.As(err, &cerr) {
				logger.FatalIf(err, "Unable to initialize the server")
			}

			// If context was canceled
			if errors.Is(err, context.Canceled) {
				logger.FatalIf(err, "Server startup canceled upon user request")
			}

			logger.LogIf(GlobalContext, err)
		}

		if !globalCLIContext.StrictS3Compat {
			logger.Info(color.RedBold("WARNING: Strict AWS S3 compatible incoming PUT, POST content payload validation is turned off, caution is advised do not use in production"))
		}
	})

	if globalActiveCred.Equal(auth.DefaultCredentials) {
		msg := fmt.Sprintf("WARNING: Detected default credentials '%s', we recommend that you change these values with 'MINIO_ROOT_USER' and 'MINIO_ROOT_PASSWORD' environment variables",
			globalActiveCred)
		logger.Info(color.RedBold(msg))
	}

	// Initialize users credentials and policies in background right after config has initialized.
	go func() {
		bootstrapTrace("globalIAMSys.Init", func() {
			globalIAMSys.Init(GlobalContext, newObject, globalEtcdClient, globalRefreshIAMInterval)
		})

		// Initialize Console UI
		if globalBrowserEnabled {
			bootstrapTrace("initConsoleServer", func() {
				srv, err := initConsoleServer()
				if err != nil {
					logger.FatalIf(err, "Unable to initialize console service")
				}

				setConsoleSrv(srv)

				go func() {
					logger.FatalIf(newConsoleServerFn().Serve(), "Unable to initialize console server")
				}()
			})
		}

		// if we see FTP args, start FTP if possible
		if len(ctx.StringSlice("ftp")) > 0 {
			bootstrapTrace("go startFTPServer", func() {
				go startFTPServer(ctx)
			})
		}

		// If we see SFTP args, start SFTP if possible
		if len(ctx.StringSlice("sftp")) > 0 {
			bootstrapTrace("go startFTPServer", func() {
				go startSFTPServer(ctx)
			})
		}
	}()

	go func() {
		if !globalDisableFreezeOnBoot {
			defer bootstrapTrace("unfreezeServices", unfreezeServices)
			t := time.AfterFunc(5*time.Minute, func() {
				logger.Info(color.Yellow("WARNING: Taking more time to initialize the config subsystem. Please set '_MINIO_DISABLE_API_FREEZE_ON_BOOT=true' to not freeze the APIs"))
			})
			defer t.Stop()
		}

		// Initialize data scanner.
		bootstrapTrace("initDataScanner", func() {
			initDataScanner(GlobalContext, newObject)
		})

		// Initialize background replication
		bootstrapTrace("initBackgroundReplication", func() {
			initBackgroundReplication(GlobalContext, newObject)
		})

		bootstrapTrace("globalTransitionState.Init", func() {
			globalTransitionState.Init(newObject)
		})

		// Initialize batch job pool.
		bootstrapTrace("newBatchJobPool", func() {
			globalBatchJobPool = newBatchJobPool(GlobalContext, newObject, 100)
		})

		// Initialize the license update job
		bootstrapTrace("initLicenseUpdateJob", func() {
			initLicenseUpdateJob(GlobalContext, newObject)
		})

		go func() {
			// Initialize transition tier configuration manager
			bootstrapTrace("globalTierConfigMgr.Init", func() {
				if err := globalTierConfigMgr.Init(GlobalContext, newObject); err != nil {
					logger.LogIf(GlobalContext, err)
				} else {
					logger.FatalIf(globalTierJournal.Init(GlobalContext), "Unable to initialize remote tier pending deletes journal")
				}
			})
		}()

		// initialize the new disk cache objects.
		if globalCacheConfig.Enabled {
			logger.Info(color.Yellow("WARNING: Drive caching is deprecated for single/multi drive MinIO setups."))
			var cacheAPI CacheObjectLayer
			cacheAPI, err = newServerCacheObjects(GlobalContext, globalCacheConfig)
			logger.FatalIf(err, "Unable to initialize drive caching")

			setCacheObjectLayer(cacheAPI)
		}

		// Initialize bucket notification system.
		bootstrapTrace("initBucketTargets", func() {
			logger.LogIf(GlobalContext, globalEventNotifier.InitBucketTargets(GlobalContext, newObject))
		})

		var buckets []BucketInfo
		// List buckets to initialize bucket metadata sub-sys.
		bootstrapTrace("listBuckets", func() {
			buckets, err = newObject.ListBuckets(GlobalContext, BucketOptions{})
			if err != nil {
				logger.LogIf(GlobalContext, fmt.Errorf("Unable to list buckets to initialize bucket metadata sub-system: %w", err))
			}
		})

		// Initialize bucket metadata sub-system.
		bootstrapTrace("globalBucketMetadataSys.Init", func() {
			globalBucketMetadataSys.Init(GlobalContext, buckets, newObject)
		})

		// initialize replication resync state.
		bootstrapTrace("initResync", func() {
			globalReplicationPool.initResync(GlobalContext, buckets, newObject)
		})

		// Initialize site replication manager after bucket metadata
		bootstrapTrace("globalSiteReplicationSys.Init", func() {
			globalSiteReplicationSys.Init(GlobalContext, newObject)
		})

		// Initialize quota manager.
		bootstrapTrace("globalBucketQuotaSys.Init", func() {
			globalBucketQuotaSys.Init(newObject)
		})

		// Populate existing buckets to the etcd backend
		if globalDNSConfig != nil {
			// Background this operation.
			bootstrapTrace("go initFederatorBackend", func() {
				go initFederatorBackend(buckets, newObject)
			})
		}

		// Prints the formatted startup message, if err is not nil then it prints additional information as well.
		printStartupMessage(getAPIEndpoints(), err)

		// Print a warning at the end of the startup banner so it is more noticeable
		if newObject.BackendInfo().StandardSCParity == 0 {
			logger.Error("Warning: The standard parity is set to 0. This can lead to data loss.")
		}
	}()

	region := globalSite.Region
	if region == "" {
		region = "us-east-1"
	}
	bootstrapTrace("globalMinioClient", func() {
		globalMinioClient, err = minio.New(globalLocalNodeName, &minio.Options{
			Creds:     credentials.NewStaticV4(globalActiveCred.AccessKey, globalActiveCred.SecretKey, ""),
			Secure:    globalIsTLS,
			Transport: globalProxyTransport,
			Region:    region,
		})
		logger.FatalIf(err, "Unable to initialize MinIO client")
	})

	// Add User-Agent to differentiate the requests.
	globalMinioClient.SetAppInfo("minio-perf-test", ReleaseTag)

	if serverDebugLog {
		logger.Info("== DEBUG Mode enabled ==")
		logger.Info("Currently set environment settings:")
		ks := []string{
			config.EnvAccessKey,
			config.EnvSecretKey,
			config.EnvRootUser,
			config.EnvRootPassword,
		}
		for _, v := range os.Environ() {
			// Do not print sensitive creds in debug.
			if slices.Contains(ks, strings.Split(v, "=")[0]) {
				continue
			}
			logger.Info(v)
		}
		logger.Info("======")
	}

	daemon.SdNotify(false, daemon.SdNotifyReady)

	<-globalOSSignalCh
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(ctx context.Context, endpointServerPools EndpointServerPools) (newObject ObjectLayer, err error) {
	return newErasureServerPools(ctx, endpointServerPools)
}
