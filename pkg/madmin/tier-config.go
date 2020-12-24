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
	"encoding/json"
	"errors"
	"log"
)

type TierType int

const (
	Unsupported TierType = iota
	S3
	Azure
	GCS
)

func (tt TierType) String() string {
	switch tt {
	case S3:
		return "s3"
	case Azure:
		return "azure"
	case GCS:
		return "gcs"
	}
	return "unsupported"
}

func (tt TierType) MarshalJSON() ([]byte, error) {
	typ := tt.String()
	return json.Marshal(typ)
}

func (tt *TierType) UnmarshalJSON(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}

	newtt, err := NewTierType(s)
	if err != nil {
		return err
	}
	*tt = newtt
	return nil
}

func NewTierType(scType string) (TierType, error) {
	switch scType {
	case S3.String():
		return S3, nil
	case Azure.String():
		return Azure, nil
	case GCS.String():
		return GCS, nil
	}

	return Unsupported, errors.New("Unsupported tier type")
}

type TierConfig struct {
	Type  TierType
	S3    *TierS3
	Azure *TierAzure
	GCS   *TierGCS
}

func (cfg *TierConfig) Endpoint() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Endpoint
	case Azure:
		return cfg.Azure.Endpoint
	case GCS:
		return cfg.GCS.Endpoint
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}

func (cfg *TierConfig) Bucket() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Bucket
	case Azure:
		return cfg.Azure.Bucket
	case GCS:
		return cfg.GCS.Bucket
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}

func (cfg *TierConfig) Prefix() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Prefix
	case Azure:
		return cfg.Azure.Prefix
	case GCS:
		return cfg.GCS.Prefix
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}

func (cfg *TierConfig) Region() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Region
	case Azure:
		return cfg.Azure.Region
	case GCS:
		return cfg.GCS.Region
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}

func (cfg *TierConfig) Name() string {
	switch cfg.Type {
	case S3:
		return cfg.S3.Name
	case Azure:
		return cfg.Azure.Name
	case GCS:
		return cfg.GCS.Name
	}
	log.Printf("unexpected tier type %s", cfg.Type)
	return ""
}
