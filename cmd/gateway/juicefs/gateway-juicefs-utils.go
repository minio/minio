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
	"fmt"
	"github.com/erikdubbelboer/gspt"
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func getMetaConf(c *cli.Context, mp string, readOnly bool) *meta.Config {
	return &meta.Config{
		Retries:    10,
		Strict:     true,
		ReadOnly:   readOnly,
		NoBGJob:    c.Bool("no-bgjob"),
		OpenCache:  time.Duration(c.Float64("open-cache") * 1e9),
		MountPoint: mp,
		Subdir:     c.String("subdir"),
	}
}

func getFormat(c *cli.Context, metaCli meta.Meta) *meta.Format {
	format, err := metaCli.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}
	if c.IsSet("bucket") {
		format.Bucket = c.String("bucket")
	}
	return format
}

func wrapRegister(mp, name string) (prometheus.Registerer, *prometheus.Registry) {
	registry := prometheus.NewRegistry() // replace default so only JuiceFS metrics are exposed
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": name}, registry))
	registerer.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registerer.MustRegister(prometheus.NewGoCollector())
	return registerer, registry
}

func exposeMetrics(c *cli.Context, m meta.Meta, registerer prometheus.Registerer, registry *prometheus.Registry) string {
	var ip, port string
	ip, port, err := net.SplitHostPort(c.String("metrics"))
	if err != nil {
		logger.Fatalf("metrics format error: %v", err)
	}

	meta.InitMetrics(registerer)
	vfs.InitMetrics(registerer)
	go metric.UpdateMetrics(m, registerer)
	http.Handle("/metrics", promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))
	registerer.MustRegister(prometheus.NewBuildInfoCollector())

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
	chunkConf := &chunk.Config{
		BlockSize: format.BlockSize * 1024,
		Compress:  format.Compression,

		GetTimeout:    time.Second * time.Duration(c.Int("get-timeout")),
		PutTimeout:    time.Second * time.Duration(c.Int("put-timeout")),
		MaxUpload:     c.Int("max-uploads"),
		Writeback:     c.Bool("writeback"),
		Prefetch:      c.Int("prefetch"),
		BufferSize:    c.Int("buffer-size") << 20,
		UploadLimit:   c.Int64("upload-limit") * 1e6 / 8,
		DownloadLimit: c.Int64("download-limit") * 1e6 / 8,

		CacheDir:       c.String("cache-dir"),
		CacheSize:      int64(c.Int("cache-size")),
		FreeSpace:      float32(c.Float64("free-space-ratio")),
		CacheMode:      os.FileMode(0600),
		CacheFullBlock: !c.Bool("cache-partial-only"),
		AutoCreate:     true,
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

func createStorage(format *meta.Format) (object.ObjectStorage, error) {
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
		blob, err = object.NewSharded(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.Shards)
	} else {
		blob, err = object.CreateStorage(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey)
	}
	if err != nil {
		return nil, err
	}
	blob = object.WithPrefix(blob, format.Name+"/")

	if format.EncryptKey != "" {
		passphrase := os.Getenv("JFS_RSA_PASSPHRASE")
		privKey, err := object.ParseRsaPrivateKeyFromPem(format.EncryptKey, passphrase)
		if err != nil {
			return nil, fmt.Errorf("load private key: %s", err)
		}
		encryptor := object.NewAESEncryptor(object.NewRSAEncryptor(privKey))
		blob = object.NewEncrypted(blob, encryptor)
	}
	return blob, nil
}
func initForSvc(c *cli.Context, mp string, metaUrl string) (meta.Meta, chunk.ChunkStore, *vfs.Config) {
	readOnly := c.Bool("read-only")
	metaConf := getMetaConf(c, mp, readOnly)
	metaCli := meta.NewClient(metaUrl, metaConf)
	format, err := metaCli.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}
	registerer, registry := wrapRegister(mp, format.Name)
	if !c.Bool("writeback") && c.IsSet("upload-delay") {
		logger.Warnf("delayed upload only work in writeback mode")
	}

	chunkConf := getChunkConf(c, format)
	blob, err := createStorage(format)
	if err != nil {
		logger.Fatalf("object storage: %s", err)
	}
	logger.Infof("Data use %s", blob)

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
	return &vfs.Config{
		Meta:       metaConf,
		Format:     format,
		Version:    version.Version(),
		Chunk:      chunkConf,
		BackupMeta: c.Duration("backup-meta"),
	}
}
func registerMetaMsg(m meta.Meta, store chunk.ChunkStore, chunkConf *chunk.Config) {
	m.OnMsg(meta.DeleteChunk, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	m.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		return vfs.Compact(*chunkConf, store, args[0].([]meta.Slice), args[1].(uint64))
	})
}

func removePassword(uri string) {
	var uri2 string
	if strings.Contains(uri, "://") {
		uri2 = utils.RemovePassword(uri)
	} else {
		uri2 = utils.RemovePassword("redis://" + uri)
	}
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
