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

type TierS3 struct {
	Name         string
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Bucket       string
	Prefix       string
	Region       string
	StorageClass string
}

type S3Options func(*TierS3) error

func S3Region(region string) func(s3 *TierS3) error {
	return func(s3 *TierS3) error {
		s3.Region = region
		return nil
	}
}

func S3Prefix(prefix string) func(s3 *TierS3) error {
	return func(s3 *TierS3) error {
		s3.Prefix = prefix
		return nil
	}
}

func S3Endpoint(endpoint string) func(s3 *TierS3) error {
	return func(s3 *TierS3) error {
		s3.Endpoint = endpoint
		return nil
	}
}

func S3StorageClass(storageClass string) func(s3 *TierS3) error {
	return func(s3 *TierS3) error {
		s3.StorageClass = storageClass
		return nil
	}
}

func NewTierS3(name, accessKey, secretKey, bucket string, options ...S3Options) (*TierConfig, error) {
	sc := &TierS3{
		Name:      name,
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
		Type: S3,
		S3:   sc,
	}, nil
}
