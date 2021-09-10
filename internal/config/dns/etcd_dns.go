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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/coredns/coredns/plugin/etcd/msg"
	"github.com/minio/minio-go/v7/pkg/set"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ErrNoEntriesFound - Indicates no entries were found for the given key (directory)
var ErrNoEntriesFound = errors.New("No entries found for this key")

// ErrDomainMissing - Indicates domain is missing
var ErrDomainMissing = errors.New("domain is missing")

const etcdPathSeparator = "/"

// create a new coredns service record for the bucket.
func newCoreDNSMsg(ip string, port string, ttl uint32, t time.Time) ([]byte, error) {
	return json.Marshal(&SrvRecord{
		Host:         ip,
		Port:         json.Number(port),
		TTL:          ttl,
		CreationDate: t,
	})
}

// Close closes the internal etcd client and cannot be used further
func (c *CoreDNS) Close() error {
	c.etcdClient.Close()
	return nil
}

// List - Retrieves list of DNS entries for the domain.
func (c *CoreDNS) List() (map[string][]SrvRecord, error) {
	var srvRecords = map[string][]SrvRecord{}
	for _, domainName := range c.domainNames {
		key := msg.Path(fmt.Sprintf("%s.", domainName), c.prefixPath)
		records, err := c.list(key+etcdPathSeparator, true)
		if err != nil {
			return srvRecords, err
		}
		for _, record := range records {
			if record.Key == "" {
				continue
			}
			srvRecords[record.Key] = append(srvRecords[record.Key], record)
		}
	}
	return srvRecords, nil
}

// Get - Retrieves DNS records for a bucket.
func (c *CoreDNS) Get(bucket string) ([]SrvRecord, error) {
	var srvRecords []SrvRecord
	for _, domainName := range c.domainNames {
		key := msg.Path(fmt.Sprintf("%s.%s.", bucket, domainName), c.prefixPath)
		records, err := c.list(key, false)
		if err != nil {
			return nil, err
		}
		// Make sure we have record.Key is empty
		// this can only happen when record.Key
		// has bucket entry with exact prefix
		// match any record.Key which do not
		// match the prefixes we skip them.
		for _, record := range records {
			if record.Key != "" {
				continue
			}
			srvRecords = append(srvRecords, record)
		}
	}
	if len(srvRecords) == 0 {
		return nil, ErrNoEntriesFound
	}
	return srvRecords, nil
}

// msgUnPath converts a etcd path to domainname.
func msgUnPath(s string) string {
	ks := strings.Split(strings.Trim(s, etcdPathSeparator), etcdPathSeparator)
	for i, j := 0, len(ks)-1; i < j; i, j = i+1, j-1 {
		ks[i], ks[j] = ks[j], ks[i]
	}
	return strings.Join(ks, ".")
}

// Retrieves list of entries under the key passed.
// Note that this method fetches entries upto only two levels deep.
func (c *CoreDNS) list(key string, domain bool) ([]SrvRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	r, err := c.etcdClient.Get(ctx, key, clientv3.WithPrefix())
	defer cancel()
	if err != nil {
		return nil, err
	}

	if r.Count == 0 {
		key = strings.TrimSuffix(key, etcdPathSeparator)
		r, err = c.etcdClient.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		// only if we are looking at `domain` as true
		// we should return error here.
		if domain && r.Count == 0 {
			return nil, ErrDomainMissing
		}
	}

	var srvRecords []SrvRecord
	for _, n := range r.Kvs {
		var srvRecord SrvRecord
		if err = json.Unmarshal(n.Value, &srvRecord); err != nil {
			return nil, err
		}
		srvRecord.Key = strings.TrimPrefix(string(n.Key), key)
		srvRecord.Key = strings.TrimSuffix(srvRecord.Key, srvRecord.Host)

		// Skip non-bucket entry like for a key
		// /skydns/net/miniocloud/10.0.0.1 that may exist as
		// dns entry for the server (rather than the bucket
		// itself).
		if srvRecord.Key == "" {
			continue
		}

		srvRecord.Key = msgUnPath(srvRecord.Key)
		srvRecords = append(srvRecords, srvRecord)

	}
	sort.Slice(srvRecords, func(i int, j int) bool {
		return srvRecords[i].Key < srvRecords[j].Key
	})
	return srvRecords, nil
}

// Put - Adds DNS entries into etcd endpoint in CoreDNS etcd message format.
func (c *CoreDNS) Put(bucket string) error {
	c.Delete(bucket) // delete any existing entries.

	t := time.Now().UTC()
	for ip := range c.domainIPs {
		bucketMsg, err := newCoreDNSMsg(ip, c.domainPort, defaultTTL, t)
		if err != nil {
			return err
		}
		for _, domainName := range c.domainNames {
			key := msg.Path(fmt.Sprintf("%s.%s", bucket, domainName), c.prefixPath)
			key = key + etcdPathSeparator + ip
			ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
			_, err = c.etcdClient.Put(ctx, key, string(bucketMsg))
			cancel()
			if err != nil {
				ctx, cancel = context.WithTimeout(context.Background(), defaultContextTimeout)
				c.etcdClient.Delete(ctx, key)
				cancel()
				return err
			}
		}
	}
	return nil
}

// Delete - Removes DNS entries added in Put().
func (c *CoreDNS) Delete(bucket string) error {
	for _, domainName := range c.domainNames {
		key := msg.Path(fmt.Sprintf("%s.%s.", bucket, domainName), c.prefixPath)
		ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
		_, err := c.etcdClient.Delete(ctx, key+etcdPathSeparator, clientv3.WithPrefix())
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteRecord - Removes a specific DNS entry
func (c *CoreDNS) DeleteRecord(record SrvRecord) error {
	for _, domainName := range c.domainNames {
		key := msg.Path(fmt.Sprintf("%s.%s.", record.Key, domainName), c.prefixPath)

		ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
		_, err := c.etcdClient.Delete(ctx, key+etcdPathSeparator+record.Host)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// String stringer name for this implementation of dns.Store
func (c *CoreDNS) String() string {
	return "etcdDNS"
}

// CoreDNS - represents dns config for coredns server.
type CoreDNS struct {
	domainNames []string
	domainIPs   set.StringSet
	domainPort  string
	prefixPath  string
	etcdClient  *clientv3.Client
}

// EtcdOption - functional options pattern style
type EtcdOption func(*CoreDNS)

// DomainNames set a list of domain names used by this CoreDNS
// client setting, note this will fail if set to empty when
// constructor initializes.
func DomainNames(domainNames []string) EtcdOption {
	return func(args *CoreDNS) {
		args.domainNames = domainNames
	}
}

// DomainIPs set a list of custom domain IPs, note this will
// fail if set to empty when constructor initializes.
func DomainIPs(domainIPs set.StringSet) EtcdOption {
	return func(args *CoreDNS) {
		args.domainIPs = domainIPs
	}
}

// DomainPort - is a string version of server port
func DomainPort(domainPort string) EtcdOption {
	return func(args *CoreDNS) {
		args.domainPort = domainPort
	}
}

// CoreDNSPath - custom prefix on etcd to populate DNS
// service records, optional and can be empty.
// if empty then c.prefixPath is used i.e "/skydns"
func CoreDNSPath(prefix string) EtcdOption {
	return func(args *CoreDNS) {
		args.prefixPath = prefix
	}
}

// NewCoreDNS - initialize a new coreDNS set/unset values.
func NewCoreDNS(cfg clientv3.Config, setters ...EtcdOption) (Store, error) {
	etcdClient, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	args := &CoreDNS{
		etcdClient: etcdClient,
	}

	for _, setter := range setters {
		setter(args)
	}

	if len(args.domainNames) == 0 || args.domainIPs.IsEmpty() {
		return nil, errors.New("invalid argument")
	}

	// strip ports off of domainIPs
	domainIPsWithoutPorts := args.domainIPs.ApplyFunc(func(ip string) string {
		host, _, err := net.SplitHostPort(ip)
		if err != nil {
			if strings.Contains(err.Error(), "missing port in address") {
				host = ip
			}
		}
		return host
	})
	args.domainIPs = domainIPsWithoutPorts

	return args, nil
}
