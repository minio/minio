/*
 * JuiceFS, Copyright 2022 Juicedata, Inc.
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

package juicefs

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/erikdubbelboer/gspt"
	"github.com/google/gops/agent"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/metric"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/usage"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/minio/cli"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/pyroscope-io/client/pyroscope"
	"github.com/sirupsen/logrus"
)

func getMetaConf(c *cli.Context, mp string, readOnly bool) *meta.Config {
	cfg := &meta.Config{
		Retries:    c.Int("io-retries"),
		Strict:     true,
		MaxDeletes: c.Int("max-deletes"),
		ReadOnly:   readOnly,
		NoBGJob:    c.Bool("no-bgjob"),
		OpenCache:  time.Duration(c.Float64("open-cache") * 1e9),
		Heartbeat:  duration(c.String("heartbeat")),
		MountPoint: mp,
		Subdir:     c.String("subdir"),
	}
	if cfg.MaxDeletes == 0 {
		logger.Warnf("Deleting object will be disabled since max-deletes is 0")
	}
	if cfg.Heartbeat < time.Second {
		logger.Warnf("heartbeat should not be less than 1 second")
		cfg.Heartbeat = time.Second
	}
	if cfg.Heartbeat > time.Minute*10 {
		logger.Warnf("heartbeat shouldd not be greater than 10 minutes")
		cfg.Heartbeat = time.Minute * 10
	}
	return cfg
}

func duration(s string) time.Duration {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Second * time.Duration(v)
	}
	if v, err := time.ParseDuration(s); err == nil {
		return v
	}
	return 0
}

func wrapRegister(mp, name string) (prometheus.Registerer, *prometheus.Registry) {
	registry := prometheus.NewRegistry() // replace default so only JuiceFS metrics are exposed
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": name}, registry))
	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewGoCollector())
	return registerer, registry
}

func exposeMetrics(c *cli.Context, m meta.Meta, registerer prometheus.Registerer, registry *prometheus.Registry) string {
	var ip, port string
	//default set
	ip, port, err := net.SplitHostPort(c.String("metrics"))
	if err != nil {
		logger.Fatalf("metrics format error: %v", err)
	}

	m.InitMetrics(registerer)
	vfs.InitMetrics(registerer)
	go metric.UpdateMetrics(m, registerer)
	http.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))
	registerer.MustRegister(collectors.NewBuildInfoCollector())

	// If not set metrics addr,the port will be auto set
	if !c.IsSet("metrics") {
		// If only set consul, ip will auto set
		if c.IsSet("consul") {
			ip, err = utils.GetLocalIp(c.String("consul"))
			if err != nil {
				logger.Errorf("Get local ip failed: %v", err)
				return ""
			}
		}
	}

	ln, err := net.Listen("tcp", net.JoinHostPort(ip, port))
	if err != nil {
		// Don't try other ports on metrics set but listen failed
		if c.IsSet("metrics") {
			logger.Errorf("listen on %s:%s failed: %v", ip, port, err)
			return ""
		}
		// Listen port on 0 will auto listen on a free port
		ln, err = net.Listen("tcp", net.JoinHostPort(ip, "0"))
		if err != nil {
			logger.Errorf("Listen failed: %v", err)
			return ""
		}
	}

	go func() {
		if err := http.Serve(ln, nil); err != nil {
			logger.Errorf("Serve for metrics: %s", err)
		}
	}()

	metricsAddr := ln.Addr().String()
	logger.Infof("Prometheus metrics listening on %s", metricsAddr)
	return metricsAddr
}
func initBackgroundTasks(c *cli.Context, vfsConf *vfs.Config, metaConf *meta.Config, m meta.Meta, blob object.ObjectStorage, registerer prometheus.Registerer, registry *prometheus.Registry) {
	metricsAddr := exposeMetrics(c, m, registerer, registry)
	if c.IsSet("consul") {
		metric.RegisterToConsul(c.String("consul"), metricsAddr, vfsConf.Meta.MountPoint)
	}
	if !metaConf.ReadOnly && !metaConf.NoBGJob && vfsConf.BackupMeta > 0 {
		go vfs.Backup(m, blob, vfsConf.BackupMeta)
	}
	if !c.Bool("no-usage-report") {
		go usage.ReportUsage(m, version.Version())
	}
}

func getChunkConf(c *cli.Context, format *meta.Format) *chunk.Config {
	cm, err := strconv.ParseUint(c.String("cache-mode"), 8, 32)
	if err != nil {
		logger.Warnf("Invalid cache-mode %s, using default value 0600", c.String("cache-mode"))
		cm = 0600
	}
	chunkConf := &chunk.Config{
		BlockSize:  format.BlockSize * 1024,
		Compress:   format.Compression,
		HashPrefix: format.HashPrefix,

		GetTimeout:    time.Second * time.Duration(c.Int("get-timeout")),
		PutTimeout:    time.Second * time.Duration(c.Int("put-timeout")),
		MaxUpload:     c.Int("max-uploads"),
		MaxRetries:    c.Int("io-retries"),
		Writeback:     c.Bool("writeback"),
		Prefetch:      c.Int("prefetch"),
		BufferSize:    c.Int("buffer-size") << 20,
		UploadLimit:   c.Int64("upload-limit") * 1e6 / 8,
		DownloadLimit: c.Int64("download-limit") * 1e6 / 8,
		UploadDelay:   duration(c.String("upload-delay")),

		CacheDir:       c.String("cache-dir"),
		CacheSize:      int64(c.Int("cache-size")),
		FreeSpace:      float32(c.Float64("free-space-ratio")),
		CacheMode:      os.FileMode(cm),
		CacheFullBlock: !c.Bool("cache-partial-only"),
		AutoCreate:     true,
	}
	if chunkConf.MaxUpload <= 0 {
		logger.Warnf("max-uploads should be greater than 0, set it to 1")
		chunkConf.MaxUpload = 1
	}
	if chunkConf.BufferSize <= 32<<20 {
		logger.Warnf("buffer-size should be more than 32 MiB")
		chunkConf.BufferSize = 32 << 20
	}

	if chunkConf.CacheDir != "memory" {
		ds := utils.SplitDir(chunkConf.CacheDir)
		for i := range ds {
			ds[i] = filepath.Join(ds[i], format.UUID)
		}
		chunkConf.CacheDir = strings.Join(ds, string(os.PathListSeparator))
	}
	return chunkConf
}

type storageHolder struct {
	object.ObjectStorage
}

func createStorage(format meta.Format) (object.ObjectStorage, error) {
	if err := format.Decrypt(); err != nil {
		return nil, fmt.Errorf("format decrypt: %s", err)
	}
	object.UserAgent = "JuiceFS-" + version.Version()
	var blob object.ObjectStorage
	var err error
	var query string
	if p := strings.Index(format.Bucket, "?"); p > 0 && p+1 < len(format.Bucket) {
		query = format.Bucket[p+1:]
		format.Bucket = format.Bucket[:p]
		logger.Debugf("query string: %s", query)
	}
	if query != "" {
		values, err := url.ParseQuery(query)
		if err != nil {
			return nil, err
		}
		var tlsSkipVerify bool
		if tlsSkipVerify, err = strconv.ParseBool(values.Get("tls-insecure-skip-verify")); err != nil {
			return nil, err
		}
		object.GetHttpClient().Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: tlsSkipVerify}
	}
	if format.Shards > 1 {
		blob, err = object.NewSharded(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken, format.Shards)
	} else {
		blob, err = object.CreateStorage(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken)
	}
	if err != nil {
		return nil, err
	}
	blob = object.WithPrefix(blob, format.Name+"/")

	if format.EncryptKey != "" {
		passphrase := os.Getenv("JFS_RSA_PASSPHRASE")
		if passphrase == "" {
			block, _ := pem.Decode([]byte(format.EncryptKey))
			// nolint:staticcheck
			if block != nil && strings.Contains(block.Headers["Proc-Type"], "ENCRYPTED") && x509.IsEncryptedPEMBlock(block) {
				return nil, fmt.Errorf("passphrase is required to private key, please try again after setting the 'JFS_RSA_PASSPHRASE' environment variable")
			}
		}

		privKey, err := object.ParseRsaPrivateKeyFromPem([]byte(format.EncryptKey), []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("parse rsa: %s", err)
		}
		encryptor := object.NewAESEncryptor(object.NewRSAEncryptor(privKey))
		blob = object.NewEncrypted(blob, encryptor)
	}
	return blob, nil
}

func NewReloadableStorage(format *meta.Format, reload func() (*meta.Format, error)) (object.ObjectStorage, error) {
	blob, err := createStorage(*format)
	if err != nil {
		return nil, err
	}
	holder := &storageHolder{blob}
	go func() {
		old := *format // keep a copy, so it will not refreshed
		for {
			time.Sleep(time.Minute)
			new, err := reload()
			if err != nil {
				logger.Warnf("reload config: %s", err)
				continue
			}
			if new.Storage != old.Storage || new.Bucket != old.Bucket || new.AccessKey != old.AccessKey || new.SecretKey != old.SecretKey {
				logger.Infof("found new configuration: storage=%s bucket=%s ak=%s", new.Storage, new.Bucket, new.AccessKey)
				newBlob, err := createStorage(*new)
				if err != nil {
					logger.Warnf("object storage: %s", err)
					continue
				}
				holder.ObjectStorage = newBlob
				old = *new
			}
		}
	}()
	return holder, nil
}

func getFormat(c *cli.Context, metaCli meta.Meta) (*meta.Format, error) {
	format, err := metaCli.Load(true)
	if err != nil {
		return nil, fmt.Errorf("load setting: %s", err)
	}
	if c.IsSet("bucket") {
		format.Bucket = c.String("bucket")
	}
	if c.IsSet("storage") {
		format.Storage = c.String("storage")
	}
	return format, nil
}

func initForSvc(c *cli.Context, mp string, metaUrl string) (meta.Meta, chunk.ChunkStore, *vfs.Config) {
	removePassword(metaUrl)
	metaConf := getMetaConf(c, mp, c.Bool("read-only"))
	metaCli := meta.NewClient(metaUrl, metaConf)
	format, err := metaCli.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}
	registerer, registry := wrapRegister(mp, format.Name)
	if !c.Bool("writeback") && c.IsSet("upload-delay") {
		logger.Warnf("delayed upload only work in writeback mode")
	}

	blob, err := NewReloadableStorage(format, func() (*meta.Format, error) {
		return getFormat(c, metaCli)
	})
	if err != nil {
		logger.Fatalf("object storage: %s", err)
	}
	logger.Infof("Data use %s", blob)

	chunkConf := getChunkConf(c, format)
	store := chunk.NewCachedStore(blob, *chunkConf, registerer)
	registerMetaMsg(metaCli, store, chunkConf)

	err = metaCli.NewSession()
	if err != nil {
		logger.Fatalf("new session: %s", err)
	}

	vfsConf := getVfsConf(c, metaConf, format, chunkConf)
	vfsConf.AccessLog = c.String("access-log")
	vfsConf.AttrTimeout = time.Millisecond * time.Duration(c.Float64("attr-cache")*1000)
	vfsConf.EntryTimeout = time.Millisecond * time.Duration(c.Float64("entry-cache")*1000)
	vfsConf.DirEntryTimeout = time.Millisecond * time.Duration(c.Float64("dir-entry-cache")*1000)

	initBackgroundTasks(c, vfsConf, metaConf, metaCli, blob, registerer, registry)

	return metaCli, store, vfsConf
}

func getVfsConf(c *cli.Context, metaConf *meta.Config, format *meta.Format, chunkConf *chunk.Config) *vfs.Config {
	cfg := &vfs.Config{
		Meta:       metaConf,
		Format:     format,
		Version:    version.Version(),
		Chunk:      chunkConf,
		BackupMeta: duration(c.String("backup-meta")),
	}
	if cfg.BackupMeta > 0 && cfg.BackupMeta < time.Minute*5 {
		logger.Fatalf("backup-meta should not be less than 5 minutes: %s", cfg.BackupMeta)
	}
	return cfg
}
func registerMetaMsg(m meta.Meta, store chunk.ChunkStore, chunkConf *chunk.Config) {
	m.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	m.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		return vfs.Compact(*chunkConf, store, args[0].([]meta.Slice), args[1].(uint64))
	})
}

func removePassword(uri string) {
	uri2 := utils.RemovePassword(uri)
	if uri2 != uri {
		for i, a := range os.Args {
			if a == uri {
				os.Args[i] = uri2
				break
			}
		}
	}
	gspt.SetProcTitle(strings.Join(os.Args, " "))
}

func setup(c *cli.Context, n int) {
	if c.NArg() < n {
		fmt.Printf("ERROR: This command requires at least %d arguments\n", n)
		fmt.Printf("USAGE:\n   juicefs %s [command options] %s\n", c.Command.Name, c.Command.ArgsUsage)
		os.Exit(1)
	}

	if c.Bool("trace") {
		utils.SetLogLevel(logrus.TraceLevel)
	} else if c.Bool("verbose") {
		utils.SetLogLevel(logrus.DebugLevel)
	} else if c.Bool("quiet") {
		utils.SetLogLevel(logrus.WarnLevel)
	} else {
		utils.SetLogLevel(logrus.InfoLevel)
	}
	if c.Bool("no-color") {
		utils.DisableLogColor()
	}

	if !c.Bool("no-agent") {
		go func() {
			for port := 6060; port < 6100; port++ {
				_ = http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", port), nil)
			}
		}()
		go func() {
			for port := 6070; port < 6100; port++ {
				_ = agent.Listen(agent.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port)})
			}
		}()
	}

	if c.IsSet("pyroscope") {
		tags := make(map[string]string)
		appName := fmt.Sprintf("juicefs.%s", c.Command.Name)
		if c.Command.Name == "mount" {
			tags["mountpoint"] = c.Args().Get(1)
		}
		if hostname, err := os.Hostname(); err == nil {
			tags["hostname"] = hostname
		}
		tags["pid"] = strconv.Itoa(os.Getpid())
		tags["version"] = version.Version()

		if _, err := pyroscope.Start(pyroscope.Config{
			ApplicationName: appName,
			ServerAddress:   c.String("pyroscope"),
			Logger:          logger,
			Tags:            tags,
			AuthToken:       os.Getenv("PYROSCOPE_AUTH_TOKEN"),
			ProfileTypes:    pyroscope.DefaultProfileTypes,
		}); err != nil {
			logger.Errorf("start pyroscope agent: %v", err)
		}
	}
}

func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:  "verbose",
			Usage: "enable debug log",
		},
		// -- quiet used by minio
		&cli.BoolFlag{
			Name:  "trace",
			Usage: "enable trace log",
		},
		&cli.BoolFlag{
			Name:  "no-agent",
			Usage: "disable pprof (:6060) and gops (:6070) agent",
		},
		&cli.StringFlag{
			Name:  "pyroscope",
			Usage: "pyroscope address",
		},
		&cli.BoolFlag{
			Name:  "no-color",
			Usage: "disable colors",
		},
	}
}

func clientFlags() []cli.Flag {
	var defaultCacheDir = "/var/jfsCache"
	switch runtime.GOOS {
	case "linux":
		if os.Getuid() == 0 {
			break
		}
		fallthrough
	case "darwin":
		fallthrough
	case "windows":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Fatalf("%v", err)
			return nil
		}
		defaultCacheDir = path.Join(homeDir, ".juicefs", "cache")
	}
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "storage",
			Usage: "customized storage type (e.g. s3, gcs, oss, cos) to access object store",
		},
		&cli.StringFlag{
			Name:  "bucket",
			Usage: "customized endpoint to access object store",
		},
		&cli.IntFlag{
			Name:  "get-timeout",
			Value: 60,
			Usage: "the max number of seconds to download an object",
		},
		&cli.IntFlag{
			Name:  "put-timeout",
			Value: 60,
			Usage: "the max number of seconds to upload an object",
		},
		&cli.IntFlag{
			Name:  "io-retries",
			Value: 10,
			Usage: "number of retries after network failure",
		},
		&cli.IntFlag{
			Name:  "max-uploads",
			Value: 20,
			Usage: "number of connections to upload",
		},
		&cli.IntFlag{
			Name:  "max-deletes",
			Value: 10,
			Usage: "number of threads to delete objects",
		},
		&cli.IntFlag{
			Name:  "buffer-size",
			Value: 300,
			Usage: "total read/write buffering in MB",
		},
		&cli.Int64Flag{
			Name:  "upload-limit",
			Value: 0,
			Usage: "bandwidth limit for upload in Mbps",
		},
		&cli.Int64Flag{
			Name:  "download-limit",
			Value: 0,
			Usage: "bandwidth limit for download in Mbps",
		},

		&cli.IntFlag{
			Name:  "prefetch",
			Value: 1,
			Usage: "prefetch N blocks in parallel",
		},
		&cli.BoolFlag{
			Name:  "writeback",
			Usage: "upload objects in background",
		},
		&cli.StringFlag{
			Name:  "upload-delay",
			Value: "0",
			Usage: "delayed duration (in seconds) for uploading objects",
		},
		&cli.StringFlag{
			Name:  "cache-dir",
			Value: defaultCacheDir,
			Usage: "directory paths of local cache, use colon to separate multiple paths",
		},
		&cli.StringFlag{
			Name:  "cache-mode",
			Value: "0600", // only owner can read/write cache
			Usage: "file permissions for cached blocks",
		},
		&cli.IntFlag{
			Name:  "cache-size",
			Value: 100 << 10,
			Usage: "size of cached objects in MiB",
		},
		&cli.Float64Flag{
			Name:  "free-space-ratio",
			Value: 0.1,
			Usage: "min free space (ratio)",
		},
		&cli.BoolFlag{
			Name:  "cache-partial-only",
			Usage: "cache only random/small read",
		},
		&cli.StringFlag{
			Name:  "backup-meta",
			Value: "3600",
			Usage: "interval (in seconds) to automatically backup metadata in the object storage (0 means disable backup)",
		},
		&cli.StringFlag{
			Name:  "heartbeat",
			Value: "12",
			Usage: "interval (in seconds) to send heartbeat; it's recommended that all clients use the same heartbeat value",
		},
		&cli.BoolFlag{
			Name:  "read-only",
			Usage: "allow lookup/read operations only",
		},
		&cli.BoolFlag{
			Name:  "no-bgjob",
			Usage: "disable background jobs (clean-up, backup, etc.)",
		},
		&cli.Float64Flag{
			Name:  "open-cache",
			Value: 0.0,
			Usage: "open files cache timeout in seconds (0 means disable this feature)",
		},
		&cli.StringFlag{
			Name:  "subdir",
			Usage: "mount a sub-directory as root",
		},
	}
}

func shareInfoFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "metrics",
			Value: "127.0.0.1:9567",
			Usage: "address to export metrics",
		},
		&cli.StringFlag{
			Name:  "consul",
			Value: "127.0.0.1:8500",
			Usage: "consul address to register",
		},
		&cli.BoolFlag{
			Name:  "no-usage-report",
			Usage: "do not send usage report",
		},
	}
}

func cacheFlags(defaultEntryCache float64) []cli.Flag {
	return []cli.Flag{
		&cli.Float64Flag{
			Name:  "attr-cache",
			Value: 1.0,
			Usage: "attributes cache timeout in seconds",
		},
		&cli.Float64Flag{
			Name:  "entry-cache",
			Value: defaultEntryCache,
			Usage: "file entry cache timeout in seconds",
		},
		&cli.Float64Flag{
			Name:  "dir-entry-cache",
			Value: 1.0,
			Usage: "dir entry cache timeout in seconds",
		},
	}
}

func expandFlags(compoundFlags [][]cli.Flag) []cli.Flag {
	var flags []cli.Flag
	for _, flag := range compoundFlags {
		flags = append(flags, flag...)
	}
	return flags
}
