/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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
 *
 */

package madmin

import (
	"context"
	"errors"
)

type StorageClassType int

const (
	unsupported StorageClassType = iota
	S3
	Azure
	Gcs
)

func (st StorageClassType) String() string {
	switch st {
	case S3:
		return "s3"
	case Azure:
		return "azure"
	case Gcs:
		return "gcs"
	}
	return "unsupported"
}

func NewStorageClassType(scType string) (StorageClassType, error) {
	switch scType {
	case S3.String():
		return S3, nil
	case Azure.String():
		return Azure, nil
	case Gcs.String():
		return Gcs, nil
	}
	return unsupported, errors.New("Unsupported storage class type")
}

type StorageClassConfig struct {
	Type  StorageClassType
	S3    *TransitionStorageClassS3
	Azure *TransitionStorageClassAzure
	Gcs   *TransitionStorageClassGcs
}

func (adm *AdminClient) AddStorageClass(ctx context.Context, cfg StorageClassConfig) error {
	// TODO: instantiate the appropriate warm backend based on storage-class type
	// use this driver to validate the creds supplied as part of storage-class config
	// on success, update tenant's storage class config store etc.
	return nil
}
