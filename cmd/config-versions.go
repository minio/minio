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

package cmd

import (
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/compress"
	xldap "github.com/minio/minio/internal/config/identity/ldap"
	"github.com/minio/minio/internal/config/identity/openid"
	"github.com/minio/minio/internal/config/notify"
	"github.com/minio/minio/internal/config/policy/opa"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/quick"
)

// FileLogger is introduced to workaround the dependency about logrus
type FileLogger struct {
	Enable   bool   `json:"enable"`
	Filename string `json:"filename"`
}

// ConsoleLogger is introduced to workaround the dependency about logrus
type ConsoleLogger struct {
	Enable bool `json:"enable"`
}

// serverConfigV33 is just like version '32', removes clientID from NATS and MQTT, and adds queueDir, queueLimit in all notification targets.
type serverConfigV33 struct {
	quick.Config `json:"-"` // ignore interfaces

	Version string `json:"version"`

	// S3 API configuration.
	Credential auth.Credentials `json:"credential"`
	Region     string           `json:"region"`
	Worm       config.BoolFlag  `json:"worm"`

	// Storage class configuration
	StorageClass storageclass.Config `json:"storageclass"`

	// Notification queue configuration.
	Notify notify.Config `json:"notify"`

	// Logger configuration
	Logger logger.Config `json:"logger"`

	// Compression configuration
	Compression compress.Config `json:"compress"`

	// OpenID configuration
	OpenID openid.Config `json:"openid"`

	// External policy enforcements.
	Policy struct {
		// OPA configuration.
		OPA opa.Args `json:"opa"`

		// Add new external policy enforcements here.
	} `json:"policy"`

	LDAPServerConfig xldap.LegacyConfig `json:"ldapserverconfig"`
}
