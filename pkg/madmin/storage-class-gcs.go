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

import "encoding/base64"

type TransitionStorageClassGcs struct {
	Name     string
	Endpoint string
	Creds    string // base64 encoding of credentials.json FIXME: TBD how do we persist gcs creds file
	Bucket   string
	Prefix   string
	Region   string
}

type GcsOptions func(*TransitionStorageClassGcs) error

func GcsPrefix(prefix string) func(*TransitionStorageClassGcs) error {
	return func(gcs *TransitionStorageClassGcs) error {
		gcs.Prefix = prefix
		return nil
	}
}

func GcsRegion(region string) func(*TransitionStorageClassGcs) error {
	return func(gcs *TransitionStorageClassGcs) error {
		gcs.Region = region
		return nil
	}
}

func (gcs *TransitionStorageClassGcs) GetCredentialJSON() ([]byte, error) {
	return base64.URLEncoding.DecodeString(gcs.Creds)
}

func NewTransitionStorageClassGcs(name string, credsJSON []byte, bucket string, options ...GcsOptions) (*TransitionStorageClassGcs, error) {
	creds := base64.URLEncoding.EncodeToString(credsJSON)
	gcs := &TransitionStorageClassGcs{
		Name:   name,
		Creds:  creds,
		Bucket: bucket,
		// Defaults
		Prefix: "",
		Region: "",
	}

	for _, option := range options {
		err := option(gcs)
		if err != nil {
			return nil, err
		}
	}

	return gcs, nil
}
