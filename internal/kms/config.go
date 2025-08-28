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

package kms

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"aead.dev/mtls"
	"github.com/minio/kms-go/kes"
	"github.com/minio/kms-go/kms"
	"github.com/minio/pkg/v3/certs"
	"github.com/minio/pkg/v3/ellipses"
	"github.com/minio/pkg/v3/env"
)

// Environment variables for MinIO KMS.
const (
	EnvKMSEndpoint   = "MINIO_KMS_SERVER"  // List of MinIO KMS endpoints, separated by ','
	EnvKMSEnclave    = "MINIO_KMS_ENCLAVE" // MinIO KMS enclave in which the key and identity exists
	EnvKMSDefaultKey = "MINIO_KMS_SSE_KEY" // Default key used for SSE-S3 or when no SSE-KMS key ID is specified
	EnvKMSAPIKey     = "MINIO_KMS_API_KEY" // Credential to access the MinIO KMS.
)

// Environment variables for MinIO KES.
const (
	EnvKESEndpoint       = "MINIO_KMS_KES_ENDPOINT"     // One or multiple KES endpoints, separated by ','
	EnvKESDefaultKey     = "MINIO_KMS_KES_KEY_NAME"     // The default key name used for IAM data and when no key ID is specified on a bucket
	EnvKESAPIKey         = "MINIO_KMS_KES_API_KEY"      // Access credential for KES - API keys and private key / certificate are mutually exclusive
	EnvKESClientKey      = "MINIO_KMS_KES_KEY_FILE"     // Path to TLS private key for authenticating to KES with mTLS - usually prefer API keys
	EnvKESClientCert     = "MINIO_KMS_KES_CERT_FILE"    // Path to TLS certificate for authenticating to KES with mTLS - usually prefer API keys
	EnvKESServerCA       = "MINIO_KMS_KES_CAPATH"       // Path to file/directory containing CA certificates to verify the KES server certificate
	EnvKESClientPassword = "MINIO_KMS_KES_KEY_PASSWORD" // Optional password to decrypt an encrypt TLS private key
)

// Environment variables for static KMS key.
const (
	EnvKMSSecretKey     = "MINIO_KMS_SECRET_KEY"      // Static KMS key in the form "<key-name>:<base64-32byte-key>". Implements a subset of KMS/KES APIs
	EnvKMSSecretKeyFile = "MINIO_KMS_SECRET_KEY_FILE" // Path to a file to read the static KMS key from
)

// EnvKMSReplicateKeyID is an env. variable that controls whether MinIO
// replicates the KMS key ID. By default, KMS key ID replication is enabled
// but can be turned off.
const EnvKMSReplicateKeyID = "MINIO_KMS_REPLICATE_KEYID"

const (
	tlsClientSessionCacheSize = 100
)

var replicateKeyID = sync.OnceValue(func() bool {
	if v, ok := os.LookupEnv(EnvKMSReplicateKeyID); ok && strings.ToLower(v) == "off" {
		return false
	}
	return true // by default, replicating KMS key IDs is enabled
})

// ReplicateKeyID reports whether KMS key IDs should be included when
// replicating objects. It's enabled by default. To disable it, set:
//
//	MINIO_KMS_REPLICATE_KEYID=off
//
// Some deployments use different KMS clusters with destinct keys on
// each site. Trying to replicate the KMS key ID can cause requests
// to fail in such setups.
func ReplicateKeyID() bool { return replicateKeyID() }

// ConnectionOptions is a structure containing options for connecting
// to a KMS.
type ConnectionOptions struct {
	CADir string // Path to directory (or file) containing CA certificates
}

// Connect returns a new Conn to a KMS. It uses configuration from the
// environment and returns a:
//
//   - connection to MinIO KMS if the "MINIO_KMS_SERVER" variable is present.
//   - connection to MinIO KES if the "MINIO_KMS_KES_ENDPOINT" is present.
//   - connection to a "local" KMS implementation using a static key if the
//     "MINIO_KMS_SECRET_KEY" or "MINIO_KMS_SECRET_KEY_FILE" is present.
//
// It returns an error if connecting to the KMS implementation fails,
// e.g. due to incomplete config, or when configurations for multiple
// KMS implementations are present.
func Connect(ctx context.Context, opts *ConnectionOptions) (*KMS, error) {
	if present, err := IsPresent(); !present || err != nil {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("kms: no KMS configuration specified")
	}

	lookup := func(key string) bool {
		_, ok := os.LookupEnv(key)
		return ok
	}
	switch {
	case lookup(EnvKMSEndpoint):
		rawEndpoint := env.Get(EnvKMSEndpoint, "")
		if rawEndpoint == "" {
			return nil, errors.New("kms: no KMS server endpoint provided")
		}
		endpoints, err := expandEndpoints(rawEndpoint)
		if err != nil {
			return nil, err
		}

		key, err := mtls.ParsePrivateKey(env.Get(EnvKMSAPIKey, ""))
		if err != nil {
			return nil, err
		}

		var rootCAs *x509.CertPool
		if opts != nil && opts.CADir != "" {
			rootCAs, err = certs.GetRootCAs(opts.CADir)
			if err != nil {
				return nil, err
			}
		}

		client, err := kms.NewClient(&kms.Config{
			Endpoints: endpoints,
			APIKey:    key,
			TLS: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
				RootCAs:            rootCAs,
			},
		})
		if err != nil {
			return nil, err
		}

		return &KMS{
			Type:       MinKMS,
			DefaultKey: env.Get(EnvKMSDefaultKey, ""),
			conn: &kmsConn{
				enclave:    env.Get(EnvKMSEnclave, ""),
				defaultKey: env.Get(EnvKMSDefaultKey, ""),
				client:     client,
			},
			latencyBuckets: defaultLatencyBuckets,
			latency:        make([]atomic.Uint64, len(defaultLatencyBuckets)),
		}, nil
	case lookup(EnvKESEndpoint):
		rawEndpoint := env.Get(EnvKESEndpoint, "")
		if rawEndpoint == "" {
			return nil, errors.New("kms: no KES server endpoint provided")
		}
		endpoints, err := expandEndpoints(rawEndpoint)
		if err != nil {
			return nil, err
		}

		conf := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
		}
		if s := env.Get(EnvKESAPIKey, ""); s != "" {
			key, err := kes.ParseAPIKey(s)
			if err != nil {
				return nil, err
			}

			cert, err := kes.GenerateCertificate(key)
			if err != nil {
				return nil, err
			}
			conf.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil }
		} else {
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
					keyBytes, err = x509.DecryptPEMBlock(privateKeyPEM, []byte(env.Get(EnvKESClientPassword, "")))
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

			certificate, err := certs.NewCertificate(env.Get(EnvKESClientCert, ""), env.Get(EnvKESClientKey, ""), loadX509KeyPair)
			if err != nil {
				return nil, err
			}
			certificate.Watch(ctx, 15*time.Minute, syscall.SIGHUP)

			conf.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert := certificate.Get()
				return &cert, nil
			}
		}

		var caDir string
		if opts != nil {
			caDir = opts.CADir
		}
		conf.RootCAs, err = certs.GetRootCAs(env.Get(EnvKESServerCA, caDir))
		if err != nil {
			return nil, err
		}

		client := kes.NewClientWithConfig("", conf)
		client.Endpoints = endpoints

		// Keep the default key in the KES cache to prevent availability issues
		// when MinIO restarts
		go func() {
			timer := time.NewTicker(10 * time.Second)
			defer timer.Stop()
			defaultKey := env.Get(EnvKESDefaultKey, "")
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					client.DescribeKey(ctx, defaultKey)
				}
			}
		}()

		return &KMS{
			Type:       MinKES,
			DefaultKey: env.Get(EnvKESDefaultKey, ""),
			conn: &kesConn{
				defaultKeyID: env.Get(EnvKESDefaultKey, ""),
				client:       client,
			},
			latencyBuckets: defaultLatencyBuckets,
			latency:        make([]atomic.Uint64, len(defaultLatencyBuckets)),
		}, nil
	default:
		var s string
		if lookup(EnvKMSSecretKeyFile) {
			b, err := os.ReadFile(env.Get(EnvKMSSecretKeyFile, ""))
			if err != nil && !os.IsNotExist(err) {
				return nil, err
			}
			if os.IsNotExist(err) {
				// Relative path where "/run/secrets" is the default docker path for secrets
				b, err = os.ReadFile(filepath.Join("/run/secrets", env.Get(EnvKMSSecretKeyFile, "")))
			}
			if err != nil {
				return nil, err
			}
			s = string(b)
		} else {
			s = env.Get(EnvKMSSecretKey, "")
		}
		return ParseSecretKey(s)
	}
}

// IsPresent reports whether a KMS configuration is present.
// It returns an error if multiple KMS configurations are
// present or if one configuration is incomplete.
func IsPresent() (bool, error) {
	// isPresent reports whether at least one of the
	// given env. variables is present.
	isPresent := func(vars ...string) bool {
		for _, v := range vars {
			if _, ok := os.LookupEnv(v); ok {
				return ok
			}
		}
		return false
	}

	// First, check which KMS/KES env. variables are present.
	// Only one set, either KMS, KES or static key must be
	// present.
	kmsPresent := isPresent(
		EnvKMSEndpoint,
		EnvKMSEnclave,
		EnvKMSAPIKey,
		EnvKMSDefaultKey,
	)
	kesPresent := isPresent(
		EnvKESEndpoint,
		EnvKESDefaultKey,
		EnvKESAPIKey,
		EnvKESClientKey,
		EnvKESClientCert,
		EnvKESClientPassword,
		EnvKESServerCA,
	)
	// We have to handle a special case for MINIO_KMS_SECRET_KEY and
	// MINIO_KMS_SECRET_KEY_FILE. The docker image always sets the
	// MINIO_KMS_SECRET_KEY_FILE - either to the argument passed to
	// the container or to a default string (e.g. "minio_master_key").
	//
	// We have to distinguish a explicit config from an implicit. Hence,
	// we unset the env. vars if they are set but empty or contain a path
	// which does not exist. The downside of this check is that if
	// MINIO_KMS_SECRET_KEY_FILE is set to a path that does not exist,
	// the server does not complain and start without a KMS config.
	//
	// Until the container image changes, this behavior has to be preserved.
	if isPresent(EnvKMSSecretKey) && os.Getenv(EnvKMSSecretKey) == "" {
		os.Unsetenv(EnvKMSSecretKey)
	}
	if isPresent(EnvKMSSecretKeyFile) {
		if filename := os.Getenv(EnvKMSSecretKeyFile); filename == "" {
			os.Unsetenv(EnvKMSSecretKeyFile)
		} else if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
			os.Unsetenv(EnvKMSSecretKeyFile)
		}
	}
	// Now, the static key env. vars are only present if they contain explicit
	// values.
	staticKeyPresent := isPresent(EnvKMSSecretKey, EnvKMSSecretKeyFile)

	switch {
	case kmsPresent && kesPresent:
		return false, errors.New("kms: configuration for MinIO KMS and MinIO KES is present")
	case kmsPresent && staticKeyPresent:
		return false, errors.New("kms: configuration for MinIO KMS and static KMS key is present")
	case kesPresent && staticKeyPresent:
		return false, errors.New("kms: configuration for MinIO KES and static KMS key is present")
	}

	// Next, we check that all required configuration for the concrete
	// KMS is present.
	// For example, the MinIO KMS requires an endpoint or a list of
	// endpoints and authentication credentials. However, a path to
	// CA certificates is optional.
	switch {
	default:
		return false, nil // No KMS config present
	case kmsPresent:
		if !isPresent(EnvKMSEndpoint) {
			return false, fmt.Errorf("kms: incomplete configuration for MinIO KMS: missing '%s'", EnvKMSEndpoint)
		}
		if !isPresent(EnvKMSEnclave) {
			return false, fmt.Errorf("kms: incomplete configuration for MinIO KMS: missing '%s'", EnvKMSEnclave)
		}
		if !isPresent(EnvKMSDefaultKey) {
			return false, fmt.Errorf("kms: incomplete configuration for MinIO KMS: missing '%s'", EnvKMSDefaultKey)
		}
		if !isPresent(EnvKMSAPIKey) {
			return false, fmt.Errorf("kms: incomplete configuration for MinIO KMS: missing '%s'", EnvKMSAPIKey)
		}
		return true, nil
	case staticKeyPresent:
		if isPresent(EnvKMSSecretKey) && isPresent(EnvKMSSecretKeyFile) {
			return false, fmt.Errorf("kms: invalid configuration for static KMS key: '%s' and '%s' are present", EnvKMSSecretKey, EnvKMSSecretKeyFile)
		}
		return true, nil
	case kesPresent:
		if !isPresent(EnvKESEndpoint) {
			return false, fmt.Errorf("kms: incomplete configuration for MinIO KES: missing '%s'", EnvKESEndpoint)
		}
		if !isPresent(EnvKESDefaultKey) {
			return false, fmt.Errorf("kms: incomplete configuration for MinIO KES: missing '%s'", EnvKESDefaultKey)
		}

		if isPresent(EnvKESClientKey, EnvKESClientCert, EnvKESClientPassword) {
			if isPresent(EnvKESAPIKey) {
				return false, fmt.Errorf("kms: invalid configuration for MinIO KES: '%s' and client certificate is present", EnvKESAPIKey)
			}
			if !isPresent(EnvKESClientCert) {
				return false, fmt.Errorf("kms: incomplete configuration for MinIO KES: missing '%s'", EnvKESClientCert)
			}
			if !isPresent(EnvKESClientKey) {
				return false, fmt.Errorf("kms: incomplete configuration for MinIO KES: missing '%s'", EnvKESClientKey)
			}
		} else if !isPresent(EnvKESAPIKey) {
			return false, errors.New("kms: incomplete configuration for MinIO KES: missing authentication method")
		}
		return true, nil
	}
}

func expandEndpoints(s string) ([]string, error) {
	var endpoints []string
	for endpoint := range strings.SplitSeq(s, ",") {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		if !ellipses.HasEllipses(endpoint) {
			endpoints = append(endpoints, endpoint)
			continue
		}

		pattern, err := ellipses.FindEllipsesPatterns(endpoint)
		if err != nil {
			return nil, fmt.Errorf("kms: invalid endpoint '%s': %v", endpoint, err)
		}
		for _, p := range pattern.Expand() {
			endpoints = append(endpoints, strings.Join(p, ""))
		}
	}
	return endpoints, nil
}
