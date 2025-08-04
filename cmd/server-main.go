// Copyright (c) 2015-2024 MinIO, Inc.
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
	"bytes"
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
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/dustin/go-humanize"
	"github.com/minio/cli"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/bucket/bandwidth"
	"github.com/minio/minio/internal/color"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/api"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/certs"
	"github.com/minio/pkg/v3/env"
	"gopkg.in/yaml.v2"
)

// ServerFlags - server command specific flags
var ServerFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "config",
		Usage:  "specify server configuration via YAML configuration",
		EnvVar: "MINIO_CONFIG",
	},
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
		Value:  time.Second * 30,
		Usage:  "shutdown timeout to gracefully shutdown server (DEPRECATED)",
		EnvVar: "MINIO_SHUTDOWN_TIMEOUT",
		Hidden: true,
	},

	cli.DurationFlag{
		Name:   "idle-timeout",
		Value:  xhttp.DefaultIdleTimeout,
		Usage:  "idle timeout is the maximum amount of time to wait for the next request when keep-alive are enabled",
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
		Usage:  "custom DNS cache TTL",
		Hidden: true,
		Value: func() time.Duration {
			if orchestrated {
				return 30 * time.Second
			}
			return 10 * time.Minute
		}(),
		EnvVar: "MINIO_DNS_CACHE_TTL",
	},
	cli.IntFlag{
		Name:   "max-idle-conns-per-host",
		Usage:  "set a custom max idle connections per host value",
		Hidden: true,
		Value:  2048,
		EnvVar: "MINIO_MAX_IDLE_CONNS_PER_HOST",
	},
	cli.StringSliceFlag{
		Name:  "ftp",
		Usage: "enable and configure an FTP(Secure) server",
	},
	cli.StringSliceFlag{
		Name:  "sftp",
		Usage: "enable and configure an SFTP server",
	},
	cli.StringFlag{
		Name:   "crossdomain-xml",
		Usage:  "provide a custom crossdomain-xml configuration to report at http://endpoint/crossdomain.xml",
		Hidden: true,
		EnvVar: "MINIO_CROSSDOMAIN_XML",
	},
	cli.StringFlag{
		Name:   "memlimit",
		Usage:  "set global memory limit per server via GOMEMLIMIT",
		Hidden: true,
		EnvVar: "MINIO_MEMLIMIT",
	},
	cli.IntFlag{
		Name:   "send-buf-size",
		Value:  4 * humanize.MiByte,
		EnvVar: "MINIO_SEND_BUF_SIZE",
		Hidden: true,
	},
	cli.IntFlag{
		Name:   "recv-buf-size",
		Value:  4 * humanize.MiByte,
		EnvVar: "MINIO_RECV_BUF_SIZE",
		Hidden: true,
	},
	cli.StringFlag{
		Name:   "log-dir",
		Usage:  "specify the directory to save the server log",
		EnvVar: "MINIO_LOG_DIR",
		Hidden: true,
	},
	cli.IntFlag{
		Name:   "log-size",
		Usage:  "specify the maximum server log file size in bytes before its rotated",
		Value:  10 * humanize.MiByte,
		EnvVar: "MINIO_LOG_SIZE",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "log-compress",
		Usage:  "specify if we want the rotated logs to be gzip compressed or not",
		EnvVar: "MINIO_LOG_COMPRESS",
		Hidden: true,
	},
	cli.StringFlag{
		Name:   "log-prefix",
		Usage:  "specify the log prefix name for the server log",
		EnvVar: "MINIO_LOG_PREFIX",
		Hidden: true,
	},
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

func configCommonToSrvCtx(cf config.ServerConfigCommon, ctxt *serverCtxt) {
	ctxt.RootUser = cf.RootUser
	ctxt.RootPwd = cf.RootPwd

	if cf.Addr != "" {
		ctxt.Addr = cf.Addr
	}
	if cf.ConsoleAddr != "" {
		ctxt.ConsoleAddr = cf.ConsoleAddr
	}
	if cf.CertsDir != "" {
		ctxt.CertsDir = cf.CertsDir
		ctxt.certsDirSet = true
	}

	if cf.Options.FTP.Address != "" {
		ctxt.FTP = append(ctxt.FTP, fmt.Sprintf("address=%s", cf.Options.FTP.Address))
	}
	if cf.Options.FTP.PassivePortRange != "" {
		ctxt.FTP = append(ctxt.FTP, fmt.Sprintf("passive-port-range=%s", cf.Options.FTP.PassivePortRange))
	}

	if cf.Options.SFTP.Address != "" {
		ctxt.SFTP = append(ctxt.SFTP, fmt.Sprintf("address=%s", cf.Options.SFTP.Address))
	}
	if cf.Options.SFTP.SSHPrivateKey != "" {
		ctxt.SFTP = append(ctxt.SFTP, fmt.Sprintf("ssh-private-key=%s", cf.Options.SFTP.SSHPrivateKey))
	}
}

func mergeServerCtxtFromConfigFile(configFile string, ctxt *serverCtxt) error {
	rd, err := xioutil.ReadFile(configFile)
	if err != nil {
		return err
	}

	cfReader := bytes.NewReader(rd)

	cv := config.ServerConfigVersion{}
	if err = yaml.Unmarshal(rd, &cv); err != nil {
		return err
	}

	switch cv.Version {
	case "v1", "v2":
	default:
		return fmt.Errorf("unexpected version: %s", cv.Version)
	}

	cfCommon := config.ServerConfigCommon{}
	if err = yaml.Unmarshal(rd, &cfCommon); err != nil {
		return err
	}

	configCommonToSrvCtx(cfCommon, ctxt)

	v, err := env.GetInt(EnvErasureSetDriveCount, 0)
	if err != nil {
		return err
	}
	setDriveCount := uint64(v)

	var pools []poolArgs
	switch cv.Version {
	case "v1":
		cfV1 := config.ServerConfigV1{}
		if err = yaml.Unmarshal(rd, &cfV1); err != nil {
			return err
		}

		pools = make([]poolArgs, 0, len(cfV1.Pools))
		for _, list := range cfV1.Pools {
			pools = append(pools, poolArgs{
				args:          list,
				setDriveCount: setDriveCount,
			})
		}
	case "v2":
		cf := config.ServerConfig{}
		cfReader.Seek(0, io.SeekStart)
		if err = yaml.Unmarshal(rd, &cf); err != nil {
			return err
		}

		pools = make([]poolArgs, 0, len(cf.Pools))
		for _, list := range cf.Pools {
			driveCount := list.SetDriveCount
			if setDriveCount > 0 {
				driveCount = setDriveCount
			}
			pools = append(pools, poolArgs{
				args:          list.Args,
				setDriveCount: driveCount,
			})
		}
	}
	ctxt.Layout, err = buildDisksLayoutFromConfFile(pools)
	return err
}

func serverHandleCmdArgs(ctxt serverCtxt) {
	handleCommonArgs(ctxt)

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

	globalEndpoints, setupType, err = createServerEndpoints(globalMinioAddr, ctxt.Layout.pools, ctxt.Layout.legacy)
	logger.FatalIf(err, "Invalid command line arguments")
	globalNodes = globalEndpoints.GetNodes()

	globalIsErasure = (setupType == ErasureSetupType)
	globalIsDistErasure = (setupType == DistErasureSetupType)
	if globalIsDistErasure {
		globalIsErasure = true
	}
	globalIsErasureSD = (setupType == ErasureSDSetupType)
	if globalDynamicAPIPort && globalIsDistErasure {
		logger.FatalIf(errInvalidArgument, "Invalid --address=\"%s\", port '0' is not allowed in a distributed erasure coded setup", ctxt.Addr)
	}

	globalLocalNodeName = GetLocalPeer(globalEndpoints, globalMinioHost, globalMinioPort)
	nodeNameSum := sha256.Sum256([]byte(globalLocalNodeName))
	globalLocalNodeNameHex = hex.EncodeToString(nodeNameSum[:])

	// Initialize, see which NIC the service is running on, and save it as global value
	setGlobalInternodeInterface(ctxt.Interface)

	globalTCPOptions = xhttp.TCPOptions{
		UserTimeout: int(ctxt.UserTimeout.Milliseconds()),
		// FIXME: Bring this back when we have valid way to handle deadlines
		//		DriveOPTimeout: globalDriveConfig.GetOPTimeout,
		Interface:   ctxt.Interface,
		SendBufSize: ctxt.SendBufSize,
		RecvBufSize: ctxt.RecvBufSize,
		IdleTimeout: ctxt.IdleTimeout,
	}

	// allow transport to be HTTP/1.1 for proxying.
	globalInternodeTransport = NewInternodeHTTPTransport(ctxt.MaxIdleConnsPerHost)()
	globalRemoteTargetTransport = NewRemoteTargetHTTPTransport(false)()
	globalProxyEndpoints = GetProxyEndpoints(globalEndpoints, globalRemoteTargetTransport)

	globalForwarder = handlers.NewForwarder(&handlers.Forwarder{
		PassHost:     true,
		RoundTripper: globalRemoteTargetTransport,
		Logger: func(err error) {
			if err != nil && !errors.Is(err, context.Canceled) {
				proxyLogIf(GlobalContext, err)
			}
		},
	})

	// On macOS, if a process already listens on LOCALIPADDR:PORT, net.Listen() falls back
	// to IPv6 address ie minio will start listening on IPv6 address whereas another
	// (non-)minio process is listening on IPv4 of given port.
	// To avoid this error situation we check for port availability.
	logger.FatalIf(xhttp.CheckPortAvailability(globalMinioHost, globalMinioPort, globalTCPOptions), "Unable to start the server")
}

func initAllSubsystems(ctx context.Context) {
	// Initialize notification peer targets
	globalNotificationSys = NewNotificationSys(globalEndpoints)

	// Create new notification system
	if globalEventNotifier == nil {
		globalEventNotifier = NewEventNotifier(GlobalContext)
	}

	// Create new bucket metadata system.
	if globalBucketMetadataSys == nil {
		globalBucketMetadataSys = NewBucketMetadataSys()
	} else {
		// Reinitialize safely when testing.
		globalBucketMetadataSys.Reset()
	}

	// Create the bucket bandwidth monitor
	globalBucketMonitor = bandwidth.NewMonitor(ctx, uint64(totalNodeCount()))

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

	// Create new bucket replication subsystem
	globalBucketTargetSys = NewBucketTargetSys(GlobalContext)

	// Create new ILM tier configuration subsystem
	globalTierConfigMgr = NewTierConfigMgr()

	globalTransitionState = newTransitionState(GlobalContext)
	globalSiteResyncMetrics = newSiteResyncMetrics(GlobalContext)
}

func configRetriableErrors(err error) bool {
	if err == nil {
		return false
	}

	notInitialized := strings.Contains(err.Error(), "Server not initialized, please try again") ||
		errors.Is(err, errServerNotInitialized)

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
		errors.Is(err, os.ErrDeadlineExceeded) ||
		notInitialized
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
		fmt.Println(time.Now().Round(time.Millisecond).Format(time.RFC3339), " bootstrap: ", msg)
	}

	noSubs := globalTrace.NumSubscribers(madmin.TraceBootstrap) == 0
	if noSubs {
		return
	}

	globalTrace.Publish(info)
}

func bootstrapTrace(msg string, worker func()) {
	if serverDebugLog {
		fmt.Println(time.Now().Round(time.Millisecond).Format(time.RFC3339), " bootstrap: ", msg)
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
		configLogIf(ctx, fmt.Errorf("Unable to initialize config, some features may be missing: %w", err))
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
	for _, ip := range localLoopbacks.ToSlice() {
		addrs.Add(net.JoinHostPort(ip, globalMinioPort))
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

var globalLoggerOutput io.WriteCloser

func initializeLogRotate(ctx *cli.Context) (io.WriteCloser, error) {
	lgDir := ctx.String("log-dir")
	if lgDir == "" {
		return os.Stderr, nil
	}
	lgDirAbs, err := filepath.Abs(lgDir)
	if err != nil {
		return nil, err
	}
	lgSize := ctx.Int("log-size")

	var fileNameFunc func() string
	if ctx.IsSet("log-prefix") {
		fileNameFunc = func() string {
			return fmt.Sprintf("%s-%s.log", ctx.String("log-prefix"), fmt.Sprintf("%X", time.Now().UTC().UnixNano()))
		}
	}

	output, err := logger.NewDir(logger.Options{
		Directory:       lgDirAbs,
		MaximumFileSize: int64(lgSize),
		Compress:        ctx.Bool("log-compress"),
		FileNameFunc:    fileNameFunc,
	})
	if err != nil {
		return nil, err
	}
	logger.EnableJSON()
	return output, nil
}

// serverMain handler called for 'minio server' command.
func serverMain(ctx *cli.Context) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	var warnings []string

	signal.Notify(globalOSSignalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go handleSignals()

	setDefaultProfilerRates()

	// Initialize globalConsoleSys system
	bootstrapTrace("newConsoleLogger", func() {
		output, err := initializeLogRotate(ctx)
		if err == nil {
			logger.Output = output
			globalConsoleSys = NewConsoleLogger(GlobalContext, output)
			globalLoggerOutput = output
		} else {
			logger.Output = os.Stderr
			globalConsoleSys = NewConsoleLogger(GlobalContext, os.Stderr)
		}
		logger.AddSystemTarget(GlobalContext, globalConsoleSys)

		// Set node name, only set for distributed setup.
		globalConsoleSys.SetNodeName(globalLocalNodeName)
		if err != nil {
			// We can only log here since we need globalConsoleSys initialized
			logger.Fatal(err, "invalid --logrorate-dir option")
		}
	})

	// Always load ENV variables from files first.
	loadEnvVarsFromFiles()

	// Handle early server environment vars
	serverHandleEarlyEnvVars()

	// Handle all server command args and build the disks layout
	bootstrapTrace("serverHandleCmdArgs", func() {
		err := buildServerCtxt(ctx, &globalServerCtxt)
		logger.FatalIf(err, "Unable to prepare the list of endpoints")

		serverHandleCmdArgs(globalServerCtxt)
	})

	// DNS cache subsystem to reduce outgoing DNS requests
	runDNSCache(ctx)

	// Handle all server environment vars.
	serverHandleEnvVars()

	// Perform any self-tests
	bootstrapTrace("selftests", func() {
		bitrotSelfTest()
		erasureSelfTest()
		compressSelfTest()
	})

	// Initialize KMS configuration
	bootstrapTrace("handleKMSConfig", handleKMSConfig)

	// Load the root credentials from the shell environment or from
	// the config file if not defined, set the default one.
	bootstrapTrace("rootCredentials", func() {
		cred := loadRootCredentials()
		if !cred.IsValid() && (env.Get(api.EnvAPIRootAccess, config.EnableOn) == config.EnableOff) {
			// Generate KMS based credentials if root access is disabled
			// and no ENV is set.
			cred = autoGenerateRootCredentials()
		}

		if !cred.IsValid() {
			cred = auth.DefaultCredentials
		}

		var err error
		globalNodeAuthToken, err = authenticateNode(cred.AccessKey, cred.SecretKey)
		if err != nil {
			logger.Fatal(err, "Unable to generate internode credentials")
		}

		globalActiveCred = cred
	})

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

	var getCert certs.GetCertificateFunc
	if globalTLSCerts != nil {
		getCert = globalTLSCerts.GetCertificate
	}

	// Check for updates in non-blocking manner.
	go func() {
		if !globalServerCtxt.Quiet && !globalInplaceUpdateDisabled {
			// Check for new updates from dl.min.io.
			bootstrapTrace("checkUpdate", func() {
				checkUpdate(getMinioMode())
			})
		}
	}()

	// Set system resources to maximum.
	bootstrapTrace("setMaxResources", func() {
		_ = setMaxResources(globalServerCtxt)
	})

	// Verify kernel release and version.
	if oldLinux() {
		warnings = append(warnings, color.YellowBold("Detected Linux kernel version older than 4.0 release, there are some known potential performance problems with this kernel version. MinIO recommends a minimum of 4.x linux kernel version for best performance"))
	}

	maxProcs := runtime.GOMAXPROCS(0)
	cpuProcs := runtime.NumCPU()
	if maxProcs < cpuProcs {
		warnings = append(warnings, color.YellowBold("Detected GOMAXPROCS(%d) < NumCPU(%d), please make sure to provide all PROCS to MinIO for optimal performance",
			maxProcs, cpuProcs))
	}

	// Initialize grid
	bootstrapTrace("initGrid", func() {
		logger.FatalIf(initGlobalGrid(GlobalContext, globalEndpoints), "Unable to configure server grid RPC services")
	})

	// Initialize lock grid
	bootstrapTrace("initLockGrid", func() {
		logger.FatalIf(initGlobalLockGrid(GlobalContext, globalEndpoints), "Unable to configure server lock grid RPC services")
	})

	// Configure server.
	bootstrapTrace("configureServer", func() {
		handler, err := configureServerHandler(globalEndpoints)
		if err != nil {
			logger.Fatal(config.ErrUnexpectedError(err), "Unable to configure one of server's RPC services")
		}
		// Allow grid to start after registering all services.
		close(globalGridStart)
		close(globalLockGridStart)

		httpServer := xhttp.NewServer(getServerListenAddrs()).
			UseHandler(setCriticalErrorHandler(corsHandler(handler))).
			UseTLSConfig(newTLSConfig(getCert)).
			UseIdleTimeout(globalServerCtxt.IdleTimeout).
			UseReadTimeout(globalServerCtxt.IdleTimeout).
			UseWriteTimeout(globalServerCtxt.IdleTimeout).
			UseReadHeaderTimeout(globalServerCtxt.ReadHeaderTimeout).
			UseBaseContext(GlobalContext).
			UseCustomLogger(log.New(io.Discard, "", 0)). // Turn-off random logging by Go stdlib
			UseTCPOptions(globalTCPOptions)

		httpServer.TCPOptions.Trace = bootstrapTraceMsg
		go func() {
			serveFn, err := httpServer.Init(GlobalContext, func(listenAddr string, err error) {
				bootLogIf(GlobalContext, fmt.Errorf("Unable to listen on `%s`: %v", listenAddr, err))
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
			if err := verifyServerSystemConfig(GlobalContext, globalEndpoints, globalGrid.Load()); err != nil {
				logger.Fatal(err, "Unable to start the server")
			}
		})
	}

	if globalEnableSyncBoot {
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

	for _, n := range globalNodes {
		nodeName := n.Host
		if n.IsLocal {
			nodeName = globalLocalNodeName
		}
		nodeNameSum := sha256.Sum256([]byte(nodeName + globalDeploymentID()))
		globalNodeNamesHex[hex.EncodeToString(nodeNameSum[:])] = struct{}{}
	}

	bootstrapTrace("waitForQuorum", func() {
		result := newObject.Health(context.Background(), HealthOptions{NoLogging: true})
		for !result.HealthyRead {
			if debugNoExit {
				logger.Info("Not waiting for quorum since we are debugging.. possible cause unhealthy sets")
				logger.Info(result.String())
				break
			}
			d := time.Duration(r.Float64() * float64(time.Second))
			logger.Info("Waiting for quorum READ healthcheck to succeed retrying in %s.. possible cause unhealthy sets", d)
			logger.Info(result.String())
			time.Sleep(d)
			result = newObject.Health(context.Background(), HealthOptions{NoLogging: true})
		}
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

			bootLogIf(GlobalContext, err)
		}

		if !globalServerCtxt.StrictS3Compat {
			warnings = append(warnings, color.YellowBold("Strict AWS S3 compatible incoming PUT, POST content payload validation is turned off, caution is advised do not use in production"))
		}
	})
	if globalActiveCred.Equal(auth.DefaultCredentials) {
		msg := fmt.Sprintf("Detected default credentials '%s', we recommend that you change these values with 'MINIO_ROOT_USER' and 'MINIO_ROOT_PASSWORD' environment variables",
			globalActiveCred)
		warnings = append(warnings, color.YellowBold(msg))
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
		if len(globalServerCtxt.FTP) > 0 {
			bootstrapTrace("go startFTPServer", func() {
				go startFTPServer(globalServerCtxt.FTP)
			})
		}

		// If we see SFTP args, start SFTP if possible
		if len(globalServerCtxt.SFTP) > 0 {
			bootstrapTrace("go startSFTPServer", func() {
				go startSFTPServer(globalServerCtxt.SFTP)
			})
		}
	}()

	go func() {
		if globalEnableSyncBoot {
			defer bootstrapTrace("unfreezeServices", unfreezeServices)
			t := time.AfterFunc(5*time.Minute, func() {
				warnings = append(warnings,
					color.YellowBold("- Initializing the config subsystem is taking longer than 5 minutes. Please remove 'MINIO_SYNC_BOOT=on' to not freeze the APIs"))
			})
			defer t.Stop()
		}

		// Initialize data scanner.
		bootstrapTrace("initDataScanner", func() {
			if v := env.Get("_MINIO_SCANNER", config.EnableOn); v == config.EnableOn {
				initDataScanner(GlobalContext, newObject)
			}
		})

		// Initialize background replication
		bootstrapTrace("initBackgroundReplication", func() {
			initBackgroundReplication(GlobalContext, newObject)
		})

		// Initialize background ILM worker poool
		bootstrapTrace("initBackgroundExpiry", func() {
			initBackgroundExpiry(GlobalContext, newObject)
		})

		bootstrapTrace("globalTransitionState.Init", func() {
			globalTransitionState.Init(newObject)
		})

		go func() {
			// Initialize transition tier configuration manager
			bootstrapTrace("globalTierConfigMgr.Init", func() {
				if err := globalTierConfigMgr.Init(GlobalContext, newObject); err != nil {
					bootLogIf(GlobalContext, err)
				}
			})
		}()

		// Initialize bucket notification system.
		bootstrapTrace("initBucketTargets", func() {
			bootLogIf(GlobalContext, globalEventNotifier.InitBucketTargets(GlobalContext, newObject))
		})

		var buckets []string
		// List buckets to initialize bucket metadata sub-sys.
		bootstrapTrace("listBuckets", func() {
			for {
				bucketsList, err := newObject.ListBuckets(GlobalContext, BucketOptions{NoMetadata: true})
				if err != nil {
					if configRetriableErrors(err) {
						logger.Info("Waiting for list buckets to succeed to initialize buckets.. possible cause (%v)", err)
						time.Sleep(time.Duration(r.Float64() * float64(time.Second)))
						continue
					}
					bootLogIf(GlobalContext, fmt.Errorf("Unable to list buckets to initialize bucket metadata sub-system: %w", err))
				}

				buckets = make([]string, len(bucketsList))
				for i := range bucketsList {
					buckets[i] = bucketsList[i].Name
				}
				break
			}
		})

		// Initialize bucket metadata sub-system.
		bootstrapTrace("globalBucketMetadataSys.Init", func() {
			globalBucketMetadataSys.Init(GlobalContext, buckets, newObject)
		})

		// initialize replication resync state.
		bootstrapTrace("initResync", func() {
			globalReplicationPool.Get().initResync(GlobalContext, buckets, newObject)
		})

		// Initialize site replication manager after bucket metadata
		bootstrapTrace("globalSiteReplicationSys.Init", func() {
			globalSiteReplicationSys.Init(GlobalContext, newObject)
		})

		// Populate existing buckets to the etcd backend
		if globalDNSConfig != nil {
			// Background this operation.
			bootstrapTrace("go initFederatorBackend", func() {
				go initFederatorBackend(buckets, newObject)
			})
		}

		// Initialize batch job pool.
		bootstrapTrace("newBatchJobPool", func() {
			globalBatchJobPool = newBatchJobPool(GlobalContext, newObject, 100)
			globalBatchJobsMetrics = batchJobMetrics{
				metrics: make(map[string]*batchJobInfo),
			}
			go globalBatchJobsMetrics.init(GlobalContext, newObject)
			go globalBatchJobsMetrics.purgeJobMetrics()
		})

		// Prints the formatted startup message, if err is not nil then it prints additional information as well.
		printStartupMessage(getAPIEndpoints(), err)

		// Print a warning at the end of the startup banner so it is more noticeable
		if newObject.BackendInfo().StandardSCParity == 0 && !globalIsErasureSD {
			warnings = append(warnings, color.YellowBold("The standard parity is set to 0. This can lead to data loss."))
		}

		for _, warn := range warnings {
			logger.Warning(warn)
		}
	}()

	region := globalSite.Region()
	if region == "" {
		region = "us-east-1"
	}
	bootstrapTrace("globalMinioClient", func() {
		globalMinioClient, err = minio.New(globalLocalNodeName, &minio.Options{
			Creds:     credentials.NewStaticV4(globalActiveCred.AccessKey, globalActiveCred.SecretKey, ""),
			Secure:    globalIsTLS,
			Transport: globalRemoteTargetTransport,
			Region:    region,
		})
		logger.FatalIf(err, "Unable to initialize MinIO client")
	})

	go bootstrapTrace("startResourceMetricsCollection", func() {
		startResourceMetricsCollection()
	})

	// Add User-Agent to differentiate the requests.
	globalMinioClient.SetAppInfo("minio-perf-test", ReleaseTag)

	if serverDebugLog {
		fmt.Println("== DEBUG Mode enabled ==")
		fmt.Println("Currently set environment settings:")
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
			fmt.Println(v)
		}
		fmt.Println("======")
	}

	daemon.SdNotify(false, daemon.SdNotifyReady)

	<-globalOSSignalCh
}

// Initialize object layer with the supplied disks, objectLayer is nil upon any error.
func newObjectLayer(ctx context.Context, endpointServerPools EndpointServerPools) (newObject ObjectLayer, err error) {
	return newErasureServerPools(ctx, endpointServerPools)
}
