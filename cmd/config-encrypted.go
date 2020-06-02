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
	"time"
	"unicode/utf8"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/madmin"
	"github.com/secure-io/sio-go"
	"github.com/secure-io/sio-go/sioutil"
)

func handleEncryptedConfigBackend(objAPI ObjectLayer, server bool) error {
	if !server {
		return nil
	}

	backend, err := readBackendEncrypted(GlobalContext, objAPI)
	if err != nil {
		return err
	}
	if backend.IsEncrypted() {
		if !globalConfigEncrypted || !globalActiveCred.IsValid() {
			return config.ErrMissingCredentialsBackendEncrypted(nil)
		}
		// We don't migrate / rotate the configuration if:
		//   1. The backend encryption has been completed (is not partial) and
		//   2. No "old" credentials are present => No secret key rotation and
		//   3. No encryption format migration is required
		if backend.IsComplete() && !globalOldCred.IsValid() && !backend.MigrationRequired() {
			return nil
		}
	}

	if !backend.IsEncrypted() {
		if !globalConfigEncrypted {
			return nil
		}
		if !globalActiveCred.IsValid() {
			return config.ErrMissingCredentialsBackendEncrypted(nil)
		}
	}

	var masterKey, oldMasterKey config.MasterKey
	masterKey, err = config.DeriveMasterKey(globalActiveCred.SecretKey, *backend.DerivationParams)
	if err != nil {
		return err
	}
	if globalOldCred.IsValid() {
		oldMasterKey, err = config.DeriveMasterKey(globalOldCred.SecretKey, *backend.DerivationParams)
		if err != nil {
			return err
		}
	}

	if backend.IsEncrypted() && (backend.MigrationRequired() || globalOldCred.IsValid()) {
		backend.Status = config.BackendEncryptionPartial
		if err := createBackendEncrypted(GlobalContext, objAPI, backend); err != nil {
			return err
		}
		if err = migrateEncryptedConfig(objAPI, masterKey, oldMasterKey, globalOldCred); err != nil {
			return err
		}
		backend.Status = config.BackendEncryptionComplete
		if err := createBackendEncrypted(GlobalContext, objAPI, backend); err != nil {
			return err
		}
		if globalOldCred.IsValid() {
			logger.Info("Rotation complete, please make sure to unset the MINIO_ACCESS_KEY_OLD and MINIO_SECRET_KEY_OLD environment variables")
		}
	} else {
		backend.Status = config.BackendEncryptionMissing
		if err := createBackendEncrypted(GlobalContext, objAPI, backend); err != nil {
			return err
		}
		if err = migratePlaintextConfig(objAPI, masterKey); err != nil {
			return err
		}
		backend.Status = config.BackendEncryptionComplete
		if err := createBackendEncrypted(GlobalContext, objAPI, backend); err != nil {
			return err
		}
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

func migratePlaintextConfig(objAPI ObjectLayer, key config.MasterKey) error {
	logger.Info("Attempting encryption of all config, IAM users and policies on MinIO backend")

	var marker string
	for {
		res, err := objAPI.ListObjects(GlobalContext, minioMetaBucket, minioConfigPrefix, marker, "", maxObjectList)
		if err != nil {
			return err
		}
		for _, obj := range res.Objects {
			var data, plaintext []byte
			data, err = readConfig(GlobalContext, objAPI, obj.Name)
			if err != nil {
				return err
			}

			// We might encounter a partial plaintext-encrypted config migration.
			// So, there are some objects that are already encrypted and some that are still
			// plaintext. But there is no way we can reliably distinguish a modified encrypted object from
			// a plaintext object. Therefore, we try to decrypt it and if that fails with NotAuthentic
			// then we just assume it's a plaintext object.
			// This is a conceptual problem which we cannot solve with code.
			plaintext, err = key.DecryptBytes(data)
			switch {
			case err == sio.NotAuthentic:
				plaintext = data
			case err != nil:
				return err
			}

			data, err = key.EncryptBytes(plaintext)
			if err != nil {
				return err
			}
			if err = saveConfig(GlobalContext, objAPI, obj.Name, data); err != nil {
				return err
			}
		}

		if !res.IsTruncated {
			break
		}
		marker = res.NextMarker
	}
	return nil
}

func migrateEncryptedConfig(objAPI ObjectLayer, key, oldKey config.MasterKey, oldCred auth.Credentials) error {
	logger.Info("Attempting (re)encryption of all config, IAM users and policies on MinIO backend")

	var marker string
	for {
		res, err := objAPI.ListObjects(GlobalContext, minioMetaBucket, minioConfigPrefix, marker, "", maxObjectList)
		if err != nil {
			return err
		}
		for _, obj := range res.Objects {
			var data, plaintext []byte
			data, err = readConfig(GlobalContext, objAPI, obj.Name)
			if err != nil {
				return err
			}

			// The data might be:
			//  - encrypted with the new/current master key (incomplete migration)
			//  - encrypted with the old master key (secret key rotation)
			//  - encrypted with the old or current secret key using the "legacy" format (migration)
			// We first try the current master key, then the old master key and finally the legacy
			// decryption scheme. We do this in this order, since trying a decryption with a master key
			// is much cheaper then trying the legacy scheme decryption. (HMAC vs. Argon2id)
			// So, the migration from "legacy" to "new" format is slightly slower - but a secret key rotation
			// is significantly faster. Since the format migration is a one-time opeation (but key rotation is not)
			// this seems justifable.
			plaintext, err = key.DecryptBytes(data)
			if err == sio.NotAuthentic {
				if oldKey != nil {
					plaintext, err = oldKey.DecryptBytes(data)
				}
				if err == sio.NotAuthentic {
					plaintext, err = decryptData(data, oldCred, globalActiveCred) // "legacy" scheme
				}
			}
			if err != nil {
				return err
			}

			data, err = key.EncryptBytes(plaintext)
			if err != nil {
				return err
			}
			if err = saveConfig(GlobalContext, objAPI, obj.Name, data); err != nil {
				return err
			}
		}

		if !res.IsTruncated {
			break
		}
		marker = res.NextMarker
	}
	return nil
}

// createBackendEncrypted creates or overwrites the 'backend-encrypted' object
// in the MinIO config meta-bucket. The 'backend-encrypted' object contains
// information about state of the configuration w.r.t. encryption. For example,
// it contains a field indicating whether the configuration is encrypted at all,
// and if so, what key derivation parameters have to be used.
func createBackendEncrypted(ctx context.Context, objAPI ObjectLayer, backend config.Backend) error {
	data, err := backend.MarshalBinary()
	if err != nil {
		return err
	}

	const (
		SHA256Sum = ""
		MD5Sum    = ""
	)
	size := int64(len(data))
	hashReader, err := hash.NewReader(bytes.NewReader(data), size, MD5Sum, SHA256Sum, size, globalCLIContext.StrictS3Compat)
	if err != nil {
		return err
	}

	_, err = objAPI.PutObject(ctx, minioMetaBucket, backendEncryptedFile, NewPutObjReader(hashReader, nil, nil), ObjectOptions{})
	return err
}

// readBackendEncrypted reads the 'backend-encrypted' object in the MinIO config
// meta-bucket and returns a new config.Backend describing the current state of
// the configuration w.r.t. encryption.
//
// If no 'backend-encrypted' object exists, readBackendEncrypted returns a new
// config.Backend indicating that the configuration is not encrypted and no error.
func readBackendEncrypted(ctx context.Context, objAPI ObjectLayer) (config.Backend, error) {
	const (
		StartOffset = 0
		Length      = -1 // unlimited
		ETag        = ""
	)

	var buffer bytes.Buffer
	if err := objAPI.GetObject(ctx, minioMetaBucket, backendEncryptedFile, StartOffset, Length, &buffer, ETag, ObjectOptions{}); err != nil {
		if isErrObjectNotFound(err) {
			salt, err := sioutil.Random(16)
			if err != nil {
				return config.Backend{}, err
			}
			return config.Backend{
				Status:           config.BackendEncryptionMissing,
				DerivationParams: config.DefaultKeyDerivationParams(salt),
			}, nil
		}

		logger.GetReqInfo(ctx).AppendTags("configFile", backendEncryptedFile)
		logger.LogIf(ctx, err)
		return config.Backend{}, err
	}
	var backend config.Backend
	err := backend.UnmarshalBinary(buffer.Bytes())
	return backend, err
}
