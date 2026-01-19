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
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/pkg/v3/wildcard"
	"gopkg.in/yaml.v3"
)

//go:generate msgp -file $GOFILE
//msgp:ignore BatchJobYamlErr

// BatchJobYamlErr can be used to return yaml validation errors with line,
// column information guiding user to fix syntax errors
type BatchJobYamlErr struct {
	line, col int
	msg       string
}

// message returns the error message excluding line, col information.
// Intended to be used in unit tests.
func (b BatchJobYamlErr) message() string {
	return b.msg
}

// Error implements Error interface
func (b BatchJobYamlErr) Error() string {
	return fmt.Sprintf("%s\n Hint: error near line: %d, col: %d", b.msg, b.line, b.col)
}

// BatchJobKV is a key-value data type which supports wildcard matching
type BatchJobKV struct {
	line, col int
	Key       string `yaml:"key" json:"key"`
	Value     string `yaml:"value" json:"value"`
}

var _ yaml.Unmarshaler = &BatchJobKV{}

// UnmarshalYAML - BatchJobKV extends default unmarshal to extract line, col information.
func (kv *BatchJobKV) UnmarshalYAML(val *yaml.Node) error {
	type jobKV BatchJobKV
	var tmp jobKV
	err := val.Decode(&tmp)
	if err != nil {
		return err
	}
	*kv = BatchJobKV(tmp)
	kv.line, kv.col = val.Line, val.Column
	return nil
}

// Validate returns an error if key is empty
func (kv BatchJobKV) Validate() error {
	if kv.Key == "" {
		return BatchJobYamlErr{
			line: kv.line,
			col:  kv.col,
			msg:  "key can't be empty",
		}
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
	line, col int
	Endpoint  string `yaml:"endpoint" json:"endpoint"`
	Token     string `yaml:"token" json:"token"`
}

var _ yaml.Unmarshaler = &BatchJobNotification{}

// UnmarshalYAML - BatchJobNotification extends unmarshal to extract line, column information
func (b *BatchJobNotification) UnmarshalYAML(val *yaml.Node) error {
	type notification BatchJobNotification
	var tmp notification
	err := val.Decode(&tmp)
	if err != nil {
		return err
	}

	*b = BatchJobNotification(tmp)
	b.line, b.col = val.Line, val.Column
	return nil
}

// BatchJobRetry stores retry configuration used in the event of failures.
type BatchJobRetry struct {
	line, col int
	Attempts  int           `yaml:"attempts" json:"attempts"` // number of retry attempts
	Delay     time.Duration `yaml:"delay" json:"delay"`       // delay between each retries
}

var _ yaml.Unmarshaler = &BatchJobRetry{}

// UnmarshalYAML - BatchJobRetry extends unmarshal to extract line, column information
func (r *BatchJobRetry) UnmarshalYAML(val *yaml.Node) error {
	type retry BatchJobRetry
	var tmp retry
	err := val.Decode(&tmp)
	if err != nil {
		return err
	}

	*r = BatchJobRetry(tmp)
	r.line, r.col = val.Line, val.Column
	return nil
}

// Validate validates input replicate retries.
func (r BatchJobRetry) Validate() error {
	if r.Attempts < 0 {
		return BatchJobYamlErr{
			line: r.line,
			col:  r.col,
			msg:  "Invalid arguments specified",
		}
	}

	if r.Delay < 0 {
		return BatchJobYamlErr{
			line: r.line,
			col:  r.col,
			msg:  "Invalid arguments specified",
		}
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
	line, col   int
	Disable     *bool   `yaml:"disable" json:"disable"`
	Batch       *int    `yaml:"batch" json:"batch"`
	InMemory    *bool   `yaml:"inmemory" json:"inmemory"`
	Compress    *bool   `yaml:"compress" json:"compress"`
	SmallerThan *string `yaml:"smallerThan" json:"smallerThan"`
	SkipErrs    *bool   `yaml:"skipErrs" json:"skipErrs"`
}

var _ yaml.Unmarshaler = &BatchJobSnowball{}

// UnmarshalYAML - BatchJobSnowball extends unmarshal to extract line, column information
func (b *BatchJobSnowball) UnmarshalYAML(val *yaml.Node) error {
	type snowball BatchJobSnowball
	var tmp snowball
	err := val.Decode(&tmp)
	if err != nil {
		return err
	}

	*b = BatchJobSnowball(tmp)
	b.line, b.col = val.Line, val.Column
	return nil
}

// Validate the snowball parameters in the job description
func (b BatchJobSnowball) Validate() error {
	if *b.Batch <= 0 {
		return BatchJobYamlErr{
			line: b.line,
			col:  b.col,
			msg:  "batch number should be non positive zero",
		}
	}
	_, err := humanize.ParseBytes(*b.SmallerThan)
	if err != nil {
		return BatchJobYamlErr{
			line: b.line,
			col:  b.col,
			msg:  err.Error(),
		}
	}
	return nil
}

// BatchJobSizeFilter supports size based filters - LesserThan and GreaterThan
type BatchJobSizeFilter struct {
	line, col  int
	UpperBound BatchJobSize `yaml:"lessThan" json:"lessThan"`
	LowerBound BatchJobSize `yaml:"greaterThan" json:"greaterThan"`
}

// UnmarshalYAML - BatchJobSizeFilter extends unmarshal to extract line, column information
func (sf *BatchJobSizeFilter) UnmarshalYAML(val *yaml.Node) error {
	type sizeFilter BatchJobSizeFilter
	var tmp sizeFilter
	err := val.Decode(&tmp)
	if err != nil {
		return err
	}

	*sf = BatchJobSizeFilter(tmp)
	sf.line, sf.col = val.Line, val.Column
	return nil
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

// Validate checks if sf is a valid batch-job size filter
func (sf BatchJobSizeFilter) Validate() error {
	if sf.LowerBound > 0 && sf.UpperBound > 0 && sf.LowerBound >= sf.UpperBound {
		return BatchJobYamlErr{
			line: sf.line,
			col:  sf.col,
			msg:  "invalid batch-job size filter",
		}
	}
	return nil
}

// BatchJobSize supports humanized byte values in yaml files type BatchJobSize uint64
type BatchJobSize int64

// UnmarshalYAML to parse humanized byte values
func (s *BatchJobSize) UnmarshalYAML(unmarshal func(any) error) error {
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
