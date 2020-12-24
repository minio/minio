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
