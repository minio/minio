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
	"errors"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
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

//   # snowball based archive transfer is by default enabled when source
//   # is local and target is remote which is also minio.
//   snowball:
//     disable: false # optionally turn-off snowball archive transfer
//     batch: 100 # upto this many objects per archive
//     inmemory: true # indicates if the archive must be staged locally or in-memory
//     compress: true # S2/Snappy compressed archive
//     smallerThan: 5MiB # create archive for all objects smaller than 5MiB
//     skipErrs: false # skips any source side read() errors

// BatchJobSnowball describes the snowball feature when replicating objects from a local source to a remote target
type BatchJobSnowball struct {
	Disable     *bool   `yaml:"disable" json:"disable"`
	Batch       *int    `yaml:"batch" json:"batch"`
	InMemory    *bool   `yaml:"inmemory" json:"inmemory"`
	Compress    *bool   `yaml:"compress" json:"compress"`
	SmallerThan *string `yaml:"smallerThan" json:"smallerThan"`
	SkipErrs    *bool   `yaml:"skipErrs" json:"skipErrs"`
}

// Validate the snowball parameters in the job description
func (b BatchJobSnowball) Validate() error {
	if *b.Batch <= 0 {
		return errors.New("batch number should be non positive zero")
	}
	_, err := humanize.ParseBytes(*b.SmallerThan)
	return err
}

// BatchJobSizeFilter supports size based filters - LesserThan and GreaterThan
type BatchJobSizeFilter struct {
	UpperBound BatchJobSize `yaml:"lesserThan,omitempty" json:"lesserThan"`
	LowerBound BatchJobSize `yaml:"greaterThan,omitempty" json:"greaterThan"`
}

// InRange returns true in the following cases and false otherwise,
// - sf.LowerBound < sz, when sf.LowerBound alone is specified
// - sz < sf.UpperBound, when sf.UpperBound alone is specified
// - sf.LowerBound < sz < sf.UpperBound when both are specified,
func (sf BatchJobSizeFilter) InRange(sz int64) bool {
	if sf.UpperBound > 0 && sz > int64(sf.UpperBound) {
		return false
	}

	if sf.LowerBound > 0 && sz < int64(sf.LowerBound) {
		return false
	}
	return true
}

var errInvalidBatchJobSizeFilter = errors.New("invalid batch-job size filter")

// Validate checks if sf is a valid batch-job size filter
func (sf BatchJobSizeFilter) Validate() error {
	if sf.LowerBound > 0 && sf.UpperBound > 0 && sf.LowerBound >= sf.UpperBound {
		return errInvalidBatchJobSizeFilter
	}
	return nil
}

// BatchJobSize supports humanized byte values in yaml files type BatchJobSize uint64
type BatchJobSize int64

// UnmarshalYAML to parse humanized byte values
func (s *BatchJobSize) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var batchExpireSz string
	err := unmarshal(&batchExpireSz)
	if err != nil {
		return err
	}
	sz, err := humanize.ParseBytes(batchExpireSz)
	if err != nil {
		return err
	}
	*s = BatchJobSize(sz)
	return nil
}
