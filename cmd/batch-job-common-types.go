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
	"strings"
	"time"

	"github.com/minio/pkg/v2/wildcard"
)

//go:generate msgp -file $GOFILE

// BatchJobKV is a key-value data type which supports wildcard matching
type BatchJobKV struct {
	Key   string `yaml:"key" json:"key"`
	Value string `yaml:"value" json:"value"`
}

// Validate returns an error if key is empty
func (kv BatchJobKV) Validate() error {
	if kv.Key == "" {
		return errInvalidArgument
	}
	return nil
}

// Empty indicates if kv is not set
func (kv BatchJobKV) Empty() bool {
	return kv.Key == "" && kv.Value == ""
}

// Match matches input kv with kv, value will be wildcard matched depending on the user input
func (kv BatchJobKV) Match(ikv BatchJobKV) bool {
	if kv.Empty() {
		return true
	}
	if strings.EqualFold(kv.Key, ikv.Key) {
		return wildcard.Match(kv.Value, ikv.Value)
	}
	return false
}

// BatchJobNotification stores notification endpoint and token information.
// Used by batch jobs to notify of their status.
type BatchJobNotification struct {
	Endpoint string `yaml:"endpoint" json:"endpoint"`
	Token    string `yaml:"token" json:"token"`
}

// BatchJobRetry stores retry configuration used in the event of failures.
type BatchJobRetry struct {
	Attempts int           `yaml:"attempts" json:"attempts"` // number of retry attempts
	Delay    time.Duration `yaml:"delay" json:"delay"`       // delay between each retries
}

// Validate validates input replicate retries.
func (r BatchJobRetry) Validate() error {
	if r.Attempts < 0 {
		return errInvalidArgument
	}

	if r.Delay < 0 {
		return errInvalidArgument
	}

	return nil
}
