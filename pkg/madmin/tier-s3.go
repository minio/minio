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

//go:generate msgp -file $GOFILE

// TierS3 represents the remote tier configuration for AWS S3 compatible backend.
type TierS3 struct {
	Endpoint     string `json:",omitempty"`
	AccessKey    string `json:",omitempty"`
	SecretKey    string `json:",omitempty"`
	Bucket       string `json:",omitempty"`
	Prefix       string `json:",omitempty"`
	Region       string `json:",omitempty"`
	StorageClass string `json:",omitempty"`
}

// S3Options supports NewTierS3 to take variadic options
type S3Options func(*TierS3) error

// S3Region helper to supply optional region to NewTierS3
func S3Region(region string) func(s3 *TierS3) error {
	return func(s3 *TierS3) error {
		s3.Region = region
		return nil
	}
}

// S3Prefix helper to supply optional object prefix to NewTierS3
func S3Prefix(prefix string) func(s3 *TierS3) error {
	return func(s3 *TierS3) error {
		s3.Prefix = prefix
		return nil
	}
}

// S3Endpoint helper to supply optional endpoint to NewTierS3
func S3Endpoint(endpoint string) func(s3 *TierS3) error {
	return func(s3 *TierS3) error {
		s3.Endpoint = endpoint
		return nil
	}
}

// S3StorageClass helper to supply optional storage class to NewTierS3
func S3StorageClass(storageClass string) func(s3 *TierS3) error {
	return func(s3 *TierS3) error {
		s3.StorageClass = storageClass
		return nil
	}
}

// NewTierS3 returns a TierConfig of S3 type. Returns error if the given
// parameters are invalid like name is empty etc.
func NewTierS3(name, accessKey, secretKey, bucket string, options ...S3Options) (*TierConfig, error) {
	if name == "" {
		return nil, ErrTierNameEmpty
	}
	sc := &TierS3{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    bucket,
		// Defaults
		Endpoint:     "https://s3.amazonaws.com",
		Region:       "",
		StorageClass: "",
	}

	for _, option := range options {
		err := option(sc)
		if err != nil {
			return nil, err
		}
	}

	return &TierConfig{
		Version: TierConfigV1,
		Type:    S3,
		Name:    name,
		S3:      sc,
	}, nil
}
