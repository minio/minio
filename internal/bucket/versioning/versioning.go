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

package versioning

import (
	"encoding/xml"
	"io"

	"github.com/minio/pkg/wildcard"
)

// State - enabled/disabled/suspended states
// for multifactor and status of versioning.
type State string

// Various supported states
const (
	Enabled State = "Enabled"
	// Disabled  State = "Disabled" only used by MFA Delete not supported yet.
	Suspended State = "Suspended"
)

var (
	errExcludedPrefixNotSupported = Errorf("excluded prefixes extension supported only when versioning is enabled")
	errTooManyExcludedPrefixes    = Errorf("too many excluded prefixes")
)

// Prefix - holds individual prefixes excluded from being versioned.
type ExcludedPrefix struct {
	Prefix string
}

// Versioning - Configuration for bucket versioning.
type Versioning struct {
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	XMLName xml.Name `xml:"VersioningConfiguration"`
	// MFADelete State    `xml:"MFADelete,omitempty"` // not supported yet.
	Status State `xml:"Status,omitempty"`
	// MinIO extension - allows selective, prefix-level versioning exclusion.
	// Requires versioning to be enabled
	ExcludedPrefixes []ExcludedPrefix `xml:",omitempty"`
}

// Validate - validates the versioning configuration
func (v Versioning) Validate() error {

	// Not supported yet
	// switch v.MFADelete {
	// case Enabled, Disabled:
	// default:
	// 	return Errorf("unsupported MFADelete state %s", v.MFADelete)
	// }
	switch v.Status {
	case Enabled:
		const maxExcludedPrefixes = 10
		if len(v.ExcludedPrefixes) > maxExcludedPrefixes {
			return errTooManyExcludedPrefixes
		}

	case Suspended:
		if len(v.ExcludedPrefixes) > 0 {
			return errExcludedPrefixNotSupported
		}
	default:
		return Errorf("unsupported Versioning status %s", v.Status)
	}
	return nil
}

// Enabled - returns true if versioning is enabled
func (v Versioning) Enabled() bool {
	return v.Status == Enabled
}

// EnabledPrefix - returns true if versioning is enabled at the bucket and given
// prefix, false otherwise.
func (v Versioning) EnabledPrefix(prefix string) bool {
	if v.Status == Enabled {
		if prefix == "" {
			return true
		}
		for _, sprefix := range v.ExcludedPrefixes {
			if matched := wildcard.MatchSimple(sprefix.Prefix, prefix); matched {
				return false
			}
		}
	}
	return true
}

// Suspended - returns true if versioning is suspended
func (v Versioning) Suspended() bool {
	return v.Status == Suspended
}

// SuspendedPrefix - returns true if versioning is suspended at the bucket level
// or suspended on the given prefix.
func (v Versioning) SuspendedPrefix(prefix string) bool {
	if v.Status == Suspended {
		return true
	}
	if v.Status == Enabled {
		if prefix == "" {
			return false
		}
		for _, sprefix := range v.ExcludedPrefixes {
			if matched := wildcard.MatchSimple(sprefix.Prefix, prefix); matched {
				return true
			}
		}
	}
	return false
}

// ParseConfig - parses data in given reader to VersioningConfiguration.
func ParseConfig(reader io.Reader) (*Versioning, error) {
	var v Versioning
	if err := xml.NewDecoder(reader).Decode(&v); err != nil {
		return nil, err
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return &v, nil
}
