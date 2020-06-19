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
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
)

func handleEncryptedConfigBackend(objAPI ObjectLayer) error {

	encrypted, err := checkBackendEncrypted(objAPI)
	if err != nil {
		return fmt.Errorf("Unable to encrypt config %w", err)
	}

	if encrypted {
		// backend is encrypted, but credentials are not specified
		// we shall fail right here. if not proceed forward.
		if !globalConfigEncrypted || !globalActiveCred.IsValid() {
			return config.ErrMissingCredentialsBackendEncrypted(nil)
		}
	} else {
		// backend is not yet encrypted, check if encryption of
		// backend is requested if not return nil and proceed
		// forward.
		if !globalConfigEncrypted {
			return nil
		}
		if !globalActiveCred.IsValid() {
			return config.ErrMissingCredentialsBackendEncrypted(nil)
		}
	}

	// Migrate IAM configuration
	if err = migrateConfigPrefixToEncrypted(objAPI, globalOldCred, encrypted); err != nil {
		return fmt.Errorf("Unable to migrate all config at .minio.sys/config/: %w", err)
	}

	return nil
}

const (
	backendEncryptedFile = "backend-encrypted"
)

var (
	backendEncryptedMigrationIncomplete = []byte("incomplete")
	backendEncryptedMigrationComplete   = []byte("encrypted")
)

func checkBackendEtcdEncrypted(ctx context.Context, client *etcd.Client) (bool, error) {
	data, err := readKeyEtcd(ctx, client, backendEncryptedFile)
	if err != nil && err != errConfigNotFound {
		return false, err
	}
	return err == nil && bytes.Equal(data, backendEncryptedMigrationComplete), nil
}

func checkBackendEncrypted(objAPI ObjectLayer) (bool, error) {
	data, err := readConfig(GlobalContext, objAPI, backendEncryptedFile)
	if err != nil && err != errConfigNotFound {
		return false, err
	}
	return err == nil && bytes.Equal(data, backendEncryptedMigrationComplete), nil
}

// decryptData - decrypts input data with more that one credentials,
func decryptData(edata []byte, creds ...auth.Credentials) ([]byte, error) {
	var err error
	var data []byte
	for _, cred := range creds {
		data, err = madmin.DecryptData(cred.String(), bytes.NewReader(edata))
		if err != nil {
			if err == madmin.ErrMaliciousData {
				continue
			}
			return nil, err
		}
		break
	}
	return data, err
}

func migrateIAMConfigsEtcdToEncrypted(ctx context.Context, client *etcd.Client) error {
	encrypted, err := checkBackendEtcdEncrypted(ctx, client)
	if err != nil {
		return err
	}

	if encrypted {
		// backend is encrypted, but credentials are not specified
		// we shall fail right here. if not proceed forward.
		if !globalConfigEncrypted || !globalActiveCred.IsValid() {
			return config.ErrMissingCredentialsBackendEncrypted(nil)
		}
	} else {
		// backend is not yet encrypted, check if encryption of
		// backend is requested if not return nil and proceed
		// forward.
		if !globalConfigEncrypted {
			return nil
		}
		if !globalActiveCred.IsValid() {
			return errInvalidArgument
		}
	}

	if encrypted {
		// No key rotation requested, and backend is
		// already encrypted. We proceed without migration.
		if !globalOldCred.IsValid() {
			return nil
		}

		// No real reason to rotate if old and new creds are same.
		if globalOldCred.Equal(globalActiveCred) {
			return nil
		}

		logger.Info("Attempting rotation of encrypted IAM users and policies on etcd with newly supplied credentials")
	} else {
		logger.Info("Attempting encryption of all IAM users and policies on etcd")
	}

	listCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	r, err := client.Get(listCtx, minioConfigPrefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	if err != nil {
		return err
	}

	if err = saveKeyEtcd(ctx, client, backendEncryptedFile, backendEncryptedMigrationIncomplete); err != nil {
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

		var data []byte
		// Is rotating of creds requested?
		if globalOldCred.IsValid() {
			data, err = decryptData(cdata, globalOldCred, globalActiveCred)
			if err != nil {
				if err == madmin.ErrMaliciousData {
					return config.ErrInvalidRotatingCredentialsBackendEncrypted(nil)
				}
				return err
			}
		} else {
			data = cdata
		}

		if !utf8.Valid(data) {
			_, err = decryptData(data, globalActiveCred)
			if err == nil {
				// Config is already encrypted with right keys
				continue
			}
			return errors.New("config data not in plain-text form or encrypted")
		}

		cencdata, err = madmin.EncryptData(globalActiveCred.String(), data)
		if err != nil {
			return err
		}

		if err = saveKeyEtcd(ctx, client, string(kv.Key), cencdata); err != nil {
			return err
		}
	}

	if encrypted && globalActiveCred.IsValid() && globalOldCred.IsValid() {
		logger.Info("Rotation complete, please make sure to unset MINIO_ACCESS_KEY_OLD and MINIO_SECRET_KEY_OLD envs")
	}

	return saveKeyEtcd(ctx, client, backendEncryptedFile, backendEncryptedMigrationComplete)
}

func migrateConfigPrefixToEncrypted(objAPI ObjectLayer, activeCredOld auth.Credentials, encrypted bool) error {
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
		logger.Info("Attempting rotation of encrypted config, IAM users and policies on MinIO with newly supplied credentials")
	} else {
		logger.Info("Attempting encryption of all config, IAM users and policies on MinIO backend")
	}

	err := saveConfig(GlobalContext, objAPI, backendEncryptedFile, backendEncryptedMigrationIncomplete)
	if err != nil {
		return err
	}

	var marker string
	for {
		res, err := objAPI.ListObjects(GlobalContext, minioMetaBucket,
			minioConfigPrefix, marker, "", maxObjectList)
		if err != nil {
			return err
		}
		for _, obj := range res.Objects {
			var (
				cdata    []byte
				cencdata []byte
			)

			cdata, err = readConfig(GlobalContext, objAPI, obj.Name)
			if err != nil {
				return err
			}

			var data []byte
			// Is rotating of creds requested?
			if activeCredOld.IsValid() {
				data, err = decryptData(cdata, activeCredOld, globalActiveCred)
				if err != nil {
					if err == madmin.ErrMaliciousData {
						return config.ErrInvalidRotatingCredentialsBackendEncrypted(nil)
					}
					return err
				}
			} else {
				data = cdata
			}

			if !utf8.Valid(data) {
				_, err = decryptData(data, globalActiveCred)
				if err == nil {
					// Config is already encrypted with right keys
					continue
				}
				return errors.New("config data not in plain-text form or encrypted")
			}

			cencdata, err = madmin.EncryptData(globalActiveCred.String(), data)
			if err != nil {
				return err
			}

			if err = saveConfig(GlobalContext, objAPI, obj.Name, cencdata); err != nil {
				return err
			}
		}

		if !res.IsTruncated {
			break
		}

		marker = res.NextMarker
	}

	if encrypted && globalActiveCred.IsValid() && activeCredOld.IsValid() {
		logger.Info("Rotation complete, please make sure to unset MINIO_ACCESS_KEY_OLD and MINIO_SECRET_KEY_OLD envs")
	}

	return saveConfig(GlobalContext, objAPI, backendEncryptedFile, backendEncryptedMigrationComplete)
}
