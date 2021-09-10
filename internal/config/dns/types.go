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

package dns

import (
	"encoding/json"
	"time"
)

const (
	defaultTTL            = 30
	defaultContextTimeout = 5 * time.Minute
)

// SrvRecord - represents a DNS service record
type SrvRecord struct {
	Host     string      `json:"host,omitempty"`
	Port     json.Number `json:"port,omitempty"`
	Priority int         `json:"priority,omitempty"`
	Weight   int         `json:"weight,omitempty"`
	Text     string      `json:"text,omitempty"`
	Mail     bool        `json:"mail,omitempty"` // Be an MX record. Priority becomes Preference.
	TTL      uint32      `json:"ttl,omitempty"`

	// Holds info about when the entry was created first.
	CreationDate time.Time `json:"creationDate"`

	// When a SRV record with a "Host: IP-address" is added, we synthesize
	// a srv.Target domain name.  Normally we convert the full Key where
	// the record lives to a DNS name and use this as the srv.Target. When
	// TargetStrip > 0 we strip the left most TargetStrip labels from the
	// DNS name.
	TargetStrip int `json:"targetstrip,omitempty"`

	// Group is used to group (or *not* to group) different services
	// together. Services with an identical Group are returned in
	// the same answer.
	Group string `json:"group,omitempty"`

	// Key carries the original key used during Put().
	Key string `json:"-"`
}
