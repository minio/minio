/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"bytes"
	"context"
	"strings"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/env"
	"github.com/minio/minio/pkg/madmin"
)

func handleEncryptedConfigBackend(objAPI ObjectLayer, server bool) error {
	if !globalConfigEncrypted {
		return nil
	}
	if !globalActiveCred.IsValid() {
		return errInvalidArgument
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	accessKeyOld := env.Get(config.EnvAccessKeyOld, "")
	secretKeyOld := env.Get(config.EnvSecretKeyOld, "")
	var activeCredOld auth.Credentials
	var err error
	if accessKeyOld != "" && secretKeyOld != "" {
		activeCredOld, err = auth.CreateCredentials(accessKeyOld, secretKeyOld)
		if err != nil {
			return err
		}
	}

	// If its server mode or nas gateway, migrate the backend.
	if server {
		// Migrating Config backend needs a retry mechanism for
		// the following reasons:
		//  - Read quorum is lost just after the initialization
		//    of the object layer.
		for range newRetryTimerSimple(doneCh) {
			// Migrate IAM configuration
			if err = migrateConfigPrefixToEncrypted(objAPI, activeCredOld); err != nil {
				if err == errDiskNotFound ||
					strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
					strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
					logger.Info("Waiting for config backend to be encrypted..")
					continue
				}
				return err
			}
			break
		}
	}
	if globalEtcdClient != nil {
		return migrateIAMConfigsEtcdToEncrypted(globalEtcdClient, activeCredOld)
	}
	return nil
}

const (
	backendEncryptedFile = "backend-encrypted"
)

var (
	backendEncryptedFileValue = []byte("encrypted")
)

func checkBackendEtcdEncrypted(ctx context.Context, client *etcd.Client) (bool, error) {
	data, err := readKeyEtcd(ctx, client, backendEncryptedFile)
	if err != nil && err != errConfigNotFound {
		return false, err
	}
	return err == nil && bytes.Equal(data, backendEncryptedFileValue), nil
}

func checkBackendEncrypted(objAPI ObjectLayer) (bool, error) {
	data, err := readConfig(context.Background(), objAPI, backendEncryptedFile)
	if err != nil && err != errConfigNotFound {
		return false, err
	}
	return err == nil && bytes.Equal(data, backendEncryptedFileValue), nil
}

func migrateIAMConfigsEtcdToEncrypted(client *etcd.Client, activeCredOld auth.Credentials) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	defer cancel()

	encrypted, err := checkBackendEtcdEncrypted(ctx, client)
	if err != nil {
		return err
	}

	if encrypted {
		// No key rotation requested, and backend is
		// already encrypted. We proceed without migration.
		if !activeCredOld.IsValid() {
			return nil
		}

		// No real reason to rotate if old and new creds are same.
		if activeCredOld.Equal(globalActiveCred) {
			return nil
		}
	}

	if !activeCredOld.IsValid() {
		logger.Info("Attempting a one time encrypt all IAM users and policies on etcd")
	} else {
		logger.Info("Attempting a rotation of encrypted IAM users and policies on etcd with newly supplied credentials")
	}

	r, err := client.Get(ctx, minioConfigPrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}
	for _, kv := range r.Kvs {
		var (
			cdata    []byte
			cencdata []byte
		)
		cdata, err = readKeyEtcd(ctx, client, string(kv.Key))
		if err != nil {
			switch err {
			case errConfigNotFound:
				// Perhaps not present or someone deleted it.
				continue
			}
			return err
		}
		// Is rotating of creds requested?
		if activeCredOld.IsValid() {
			cdata, err = madmin.DecryptData(activeCredOld.String(), bytes.NewReader(cdata))
			if err != nil {
				return err
			}
		}

		cencdata, err = madmin.EncryptData(globalActiveCred.String(), cdata)
		if err != nil {
			return err
		}

		if err = saveKeyEtcd(ctx, client, string(kv.Key), cencdata); err != nil {
			return err
		}
	}
	return saveKeyEtcd(ctx, client, backendEncryptedFile, backendEncryptedFileValue)
}

func migrateConfigPrefixToEncrypted(objAPI ObjectLayer, activeCredOld auth.Credentials) error {
	encrypted, err := checkBackendEncrypted(objAPI)
	if err != nil {
		return err
	}

	if encrypted {
		// No key rotation requested, and backend is
		// already encrypted. We proceed without migration.
		if !activeCredOld.IsValid() {
			return nil
		}

		// No real reason to rotate if old and new creds are same.
		if activeCredOld.Equal(globalActiveCred) {
			return nil
		}
	}

	if !activeCredOld.IsValid() {
		logger.Info("Attempting a one time encrypt of all config, IAM users and policies on MinIO backend")
	} else {
		logger.Info("Attempting a rotation of encrypted config, IAM users and policies on MinIO with newly supplied credentials")
	}

	// Construct path to config/.transaction for locking
	transactionConfigPrefix := minioConfigPrefix + "/transaction.lock"

	// As object layer's GetObject() and PutObject() take respective lock on minioMetaBucket
	// and configFile, take a transaction lock to avoid data race between readConfig()
	// and saveConfig().
	objLock := globalNSMutex.NewNSLock(context.Background(), minioMetaBucket, transactionConfigPrefix)
	if err := objLock.GetLock(globalOperationTimeout); err != nil {
		return err
	}
	defer objLock.Unlock()

	var marker string
	for {
		res, err := objAPI.ListObjects(context.Background(), minioMetaBucket, minioConfigPrefix, marker, "", 1000)
		if err != nil {
			return err
		}
		for _, obj := range res.Objects {
			var (
				cdata    []byte
				cencdata []byte
			)

			cdata, err = readConfig(context.Background(), objAPI, obj.Name)
			if err != nil {
				return err
			}

			// Is rotating of creds requested?
			if activeCredOld.IsValid() {
				cdata, err = madmin.DecryptData(activeCredOld.String(), bytes.NewReader(cdata))
				if err != nil {
					return err
				}
			}

			cencdata, err = madmin.EncryptData(globalActiveCred.String(), cdata)
			if err != nil {
				return err
			}

			if err = saveConfig(context.Background(), objAPI, obj.Name, cencdata); err != nil {
				return err
			}
		}

		if !res.IsTruncated {
			break
		}

		marker = res.NextMarker
	}

	return saveConfig(context.Background(), objAPI, backendEncryptedFile, backendEncryptedFileValue)
}
