// Copyright (c) 2015-2021 MinIO, Inc.
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
