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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/pkg/set"

	"github.com/coredns/coredns/plugin/etcd/msg"
	etcd "github.com/coreos/etcd/clientv3"
)

// ErrNoEntriesFound - Indicates no entries were found for the given key (directory)
var ErrNoEntriesFound = errors.New("No entries found for this key")

// create a new coredns service record for the bucket.
func newCoreDNSMsg(bucket, ip string, port int, ttl uint32) ([]byte, error) {
	return json.Marshal(&SrvRecord{
		Host:         ip,
		Port:         port,
		TTL:          ttl,
		CreationDate: time.Now().UTC(),
	})
}

// Retrieves list of DNS entries for the domain.
func (c *coreDNS) List() ([]SrvRecord, error) {
	key := msg.Path(fmt.Sprintf("%s.", c.domainName), defaultPrefixPath)
	return c.list(key)
}

// Retrieves DNS records for a bucket.
func (c *coreDNS) Get(bucket string) ([]SrvRecord, error) {
	key := msg.Path(fmt.Sprintf("%s.%s.", bucket, c.domainName), defaultPrefixPath)
	return c.list(key)
}

// Retrieves list of entries under the key passed.
// Note that this method fetches entries upto only two levels deep.
func (c *coreDNS) list(key string) ([]SrvRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	r, err := c.etcdClient.Get(ctx, key, etcd.WithPrefix())
	defer cancel()
	if err != nil {
		return nil, err
	}
	if r.Count == 0 {
		key = strings.TrimSuffix(key, "/")
		r, err = c.etcdClient.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if r.Count == 0 {
			return nil, ErrNoEntriesFound
		}
	}

	var srvRecords []SrvRecord
	for _, n := range r.Kvs {
		var srvRecord SrvRecord
		if err = json.Unmarshal([]byte(n.Value), &srvRecord); err != nil {
			return nil, err
		}
		srvRecord.Key = strings.TrimPrefix(string(n.Key), key)
		srvRecord.Key = strings.TrimSuffix(srvRecord.Key, srvRecord.Host)
		// SRV records are stored in the following form
		// /skydns/net/miniocloud/bucket1, so this function serves multiple
		// purposes basically when we do a Get(bucketName) this function
		// should return a single DNS record for any input 'bucketName'.
		//
		// In all other situations when we want to list all DNS records,
		// which is handled in the else clause.
		if key != msg.Path(fmt.Sprintf(".%s.", c.domainName), defaultPrefixPath) {
			if srvRecord.Key == "/" {
				srvRecords = append(srvRecords, srvRecord)
			}
		} else {
			srvRecords = append(srvRecords, srvRecord)
		}

	}
	if len(srvRecords) == 0 {
		return nil, ErrNoEntriesFound
	}
	sort.Slice(srvRecords, func(i int, j int) bool {
		return srvRecords[i].Key < srvRecords[j].Key
	})
	return srvRecords, nil
}

// Adds DNS entries into etcd endpoint in CoreDNS etcd message format.
func (c *coreDNS) Put(bucket string) error {
	for ip := range c.domainIPs {
		bucketMsg, err := newCoreDNSMsg(bucket, ip, c.domainPort, defaultTTL)
		if err != nil {
			return err
		}
		key := msg.Path(fmt.Sprintf("%s.%s", bucket, c.domainName), defaultPrefixPath)
		key = key + "/" + ip
		ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
		_, err = c.etcdClient.Put(ctx, key, string(bucketMsg))
		defer cancel()
		if err != nil {
			ctx, cancel = context.WithTimeout(context.Background(), defaultContextTimeout)
			c.etcdClient.Delete(ctx, key)
			defer cancel()
			return err
		}
	}
	return nil
}

// Removes DNS entries added in Put().
func (c *coreDNS) Delete(bucket string) error {
	key := msg.Path(fmt.Sprintf("%s.%s.", bucket, c.domainName), defaultPrefixPath)
	srvRecords, err := c.list(key)
	if err != nil {
		return err
	}
	for _, record := range srvRecords {
		dctx, dcancel := context.WithTimeout(context.Background(), defaultContextTimeout)
		if _, err = c.etcdClient.Delete(dctx, key+"/"+record.Host); err != nil {
			dcancel()
			return err
		}
		dcancel()
	}
	return err
}

// CoreDNS - represents dns config for coredns server.
type coreDNS struct {
	domainName string
	domainIPs  set.StringSet
	domainPort int
	etcdClient *etcd.Client
}

// NewCoreDNS - initialize a new coreDNS set/unset values.
func NewCoreDNS(domainName string, domainIPs set.StringSet, domainPort string, etcdClient *etcd.Client) (Config, error) {
	if domainName == "" || domainIPs.IsEmpty() {
		return nil, errors.New("invalid argument")
	}

	port, err := strconv.Atoi(domainPort)
	if err != nil {
		return nil, err
	}

	return &coreDNS{
		domainName: domainName,
		domainIPs:  domainIPs,
		domainPort: port,
		etcdClient: etcdClient,
	}, nil
}
