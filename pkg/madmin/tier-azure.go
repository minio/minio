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

// TierAzure represents the remote tier configuration for Azure Blob Storage.
type TierAzure struct {
	Endpoint     string `json:",omitempty"`
	AccountName  string `json:",omitempty"`
	AccountKey   string `json:",omitempty"`
	Bucket       string `json:",omitempty"`
	Prefix       string `json:",omitempty"`
	Region       string `json:",omitempty"`
	StorageClass string `json:",omitempty"`
}

// AzureOptions supports NewTierAzure to take variadic options
type AzureOptions func(*TierAzure) error

// AzurePrefix helper to supply optional object prefix to NewTierAzure
func AzurePrefix(prefix string) func(az *TierAzure) error {
	return func(az *TierAzure) error {
		az.Prefix = prefix
		return nil
	}
}

// AzureEndpoint helper to supply optional endpoint to NewTierAzure
func AzureEndpoint(endpoint string) func(az *TierAzure) error {
	return func(az *TierAzure) error {
		az.Endpoint = endpoint
		return nil
	}
}

// AzureRegion helper to supply optional region to NewTierAzure
func AzureRegion(region string) func(az *TierAzure) error {
	return func(az *TierAzure) error {
		az.Region = region
		return nil
	}
}

// AzureStorageClass helper to supply optional storage class to NewTierAzure
func AzureStorageClass(sc string) func(az *TierAzure) error {
	return func(az *TierAzure) error {
		az.StorageClass = sc
		return nil
	}
}

// NewTierAzure returns a TierConfig of Azure type. Returns error if the given
// parameters are invalid like name is empty etc.
func NewTierAzure(name, accountName, accountKey, bucket string, options ...AzureOptions) (*TierConfig, error) {
	if name == "" {
		return nil, ErrTierNameEmpty
	}

	az := &TierAzure{
		AccountName: accountName,
		AccountKey:  accountKey,
		Bucket:      bucket,
		// Defaults
		Endpoint:     "http://blob.core.windows.net",
		Prefix:       "",
		Region:       "",
		StorageClass: "",
	}

	for _, option := range options {
		err := option(az)
		if err != nil {
			return nil, err
		}
	}

	return &TierConfig{
		Version: TierConfigV1,
		Type:    Azure,
		Name:    name,
		Azure:   az,
	}, nil
}
