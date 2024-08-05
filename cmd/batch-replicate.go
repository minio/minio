// Copyright (c) 2015-2023 MinIO, Inc.
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

package cmd

import (
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/pkg/v3/xtime"
)

//go:generate msgp -file $GOFILE

// replicate:
//   # source of the objects to be replicated
//   source:
//     type: "minio"
//     bucket: "testbucket"
//     prefix: "spark/"
//
//   # optional flags based filtering criteria
//   # for source objects
//   flags:
//     filter:
//       newerThan: "7d"
//       olderThan: "7d"
//       createdAfter: "date"
//       createdBefore: "date"
//       tags:
//         - key: "name"
//           value: "value*"
//       metadata:
//         - key: "content-type"
//           value: "image/*"
//     notify:
//       endpoint: "https://splunk-hec.dev.com"
//       token: "Splunk ..." # e.g. "Bearer token"
//
//   # target where the objects must be replicated
//   target:
//     type: "minio"
//     bucket: "testbucket1"
//     endpoint: "https://play.min.io"
//     path: "on"
//     credentials:
//       accessKey: "minioadmin"
//       secretKey: "minioadmin"
//       sessionToken: ""

// BatchReplicateFilter holds all the filters currently supported for batch replication
type BatchReplicateFilter struct {
	NewerThan     xtime.Duration `yaml:"newerThan,omitempty" json:"newerThan"`
	OlderThan     xtime.Duration `yaml:"olderThan,omitempty" json:"olderThan"`
	CreatedAfter  time.Time      `yaml:"createdAfter,omitempty" json:"createdAfter"`
	CreatedBefore time.Time      `yaml:"createdBefore,omitempty" json:"createdBefore"`
	Tags          []BatchJobKV   `yaml:"tags,omitempty" json:"tags"`
	Metadata      []BatchJobKV   `yaml:"metadata,omitempty" json:"metadata"`
}

// BatchJobReplicateFlags various configurations for replication job definition currently includes
// - filter
// - notify
// - retry
type BatchJobReplicateFlags struct {
	Filter BatchReplicateFilter `yaml:"filter" json:"filter"`
	Notify BatchJobNotification `yaml:"notify" json:"notify"`
	Retry  BatchJobRetry        `yaml:"retry" json:"retry"`
}

// BatchJobReplicateResourceType defines the type of batch jobs
type BatchJobReplicateResourceType string

// Validate validates if the replicate resource type is recognized and supported
func (t BatchJobReplicateResourceType) Validate() error {
	switch t {
	case BatchJobReplicateResourceMinIO:
	case BatchJobReplicateResourceS3:
	default:
		return errInvalidArgument
	}
	return nil
}

func (t BatchJobReplicateResourceType) isMinio() bool {
	return t == BatchJobReplicateResourceMinIO
}

// Different types of batch jobs..
const (
	BatchJobReplicateResourceMinIO BatchJobReplicateResourceType = "minio"
	BatchJobReplicateResourceS3    BatchJobReplicateResourceType = "s3"

	// add future targets
)

// BatchJobReplicateCredentials access credentials for batch replication it may
// be either for target or source.
type BatchJobReplicateCredentials struct {
	AccessKey    string `xml:"AccessKeyId" json:"accessKey,omitempty" yaml:"accessKey"`
	SecretKey    string `xml:"SecretAccessKey" json:"secretKey,omitempty" yaml:"secretKey"`
	SessionToken string `xml:"SessionToken" json:"sessionToken,omitempty" yaml:"sessionToken"`
}

// Empty indicates if credentials are not set
func (c BatchJobReplicateCredentials) Empty() bool {
	return c.AccessKey == "" && c.SecretKey == "" && c.SessionToken == ""
}

// Validate validates if credentials are valid
func (c BatchJobReplicateCredentials) Validate() error {
	if !auth.IsAccessKeyValid(c.AccessKey) || !auth.IsSecretKeyValid(c.SecretKey) {
		return errInvalidArgument
	}
	return nil
}

// BatchJobReplicateTarget describes target element of the replication job that receives
// the filtered data from source
type BatchJobReplicateTarget struct {
	Type     BatchJobReplicateResourceType `yaml:"type" json:"type"`
	Bucket   string                        `yaml:"bucket" json:"bucket"`
	Prefix   string                        `yaml:"prefix" json:"prefix"`
	Endpoint string                        `yaml:"endpoint" json:"endpoint"`
	Path     string                        `yaml:"path" json:"path"`
	Creds    BatchJobReplicateCredentials  `yaml:"credentials" json:"credentials"`
}

// ValidPath returns true if path is valid
func (t BatchJobReplicateTarget) ValidPath() bool {
	return t.Path == "on" || t.Path == "off" || t.Path == "auto" || t.Path == ""
}

// BatchJobReplicateSource describes source element of the replication job that is
// the source of the data for the target
type BatchJobReplicateSource struct {
	Type     BatchJobReplicateResourceType `yaml:"type" json:"type"`
	Bucket   string                        `yaml:"bucket" json:"bucket"`
	Prefix   BatchJobPrefix                `yaml:"prefix" json:"prefix"`
	Endpoint string                        `yaml:"endpoint" json:"endpoint"`
	Path     string                        `yaml:"path" json:"path"`
	Creds    BatchJobReplicateCredentials  `yaml:"credentials" json:"credentials"`
	Snowball BatchJobSnowball              `yaml:"snowball" json:"snowball"`
}

// ValidPath returns true if path is valid
func (s BatchJobReplicateSource) ValidPath() bool {
	switch s.Path {
	case "on", "off", "auto", "":
		return true
	default:
		return false
	}
}

// BatchJobReplicateV1 v1 of batch job replication
type BatchJobReplicateV1 struct {
	APIVersion string                  `yaml:"apiVersion" json:"apiVersion"`
	Flags      BatchJobReplicateFlags  `yaml:"flags" json:"flags"`
	Target     BatchJobReplicateTarget `yaml:"target" json:"target"`
	Source     BatchJobReplicateSource `yaml:"source" json:"source"`

	clnt *miniogo.Core `msg:"-"`
}

// RemoteToLocal returns true if source is remote and target is local
func (r BatchJobReplicateV1) RemoteToLocal() bool {
	return !r.Source.Creds.Empty()
}
