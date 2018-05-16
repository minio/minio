/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
 */

package dns

import (
	"time"
)

const (
	defaultTTL            = 30
	defaultPrefixPath     = "/skydns"
	defaultContextTimeout = 5 * time.Minute
)

// SrvRecord - represents a DNS service record
type SrvRecord struct {
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Priority int    `json:"priority,omitempty"`
	Weight   int    `json:"weight,omitempty"`
	Text     string `json:"text,omitempty"`
	Mail     bool   `json:"mail,omitempty"` // Be an MX record. Priority becomes Preference.
	TTL      uint32 `json:"ttl,omitempty"`

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

// Config - represents dns put, get interface. This interface can be
// used to implement various backends as needed.
type Config interface {
	Put(key string) error
	List() ([]SrvRecord, error)
	Get(key string) ([]SrvRecord, error)
	Delete(key string) error
}
