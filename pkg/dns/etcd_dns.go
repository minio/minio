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
	etcd "github.com/coreos/etcd/client"
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
	kapi := etcd.NewKeysAPI(c.etcdClient)
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	r, err := kapi.Get(ctx, key, &etcd.GetOptions{Recursive: true})
	cancel()
	if err != nil {
		return nil, err
	}

	var srvRecords []SrvRecord
	for _, n := range r.Node.Nodes {
		if !n.Dir {
			var srvRecord SrvRecord
			if err = json.Unmarshal([]byte(n.Value), &srvRecord); err != nil {
				return nil, err
			}
			srvRecord.Key = strings.TrimPrefix(n.Key, key)
			srvRecords = append(srvRecords, srvRecord)
		} else {
			// As this is a directory, loop through all the nodes inside (assuming all nodes are non-directories)
			for _, n1 := range n.Nodes {
				var srvRecord SrvRecord
				if err = json.Unmarshal([]byte(n1.Value), &srvRecord); err != nil {
					return nil, err
				}
				srvRecord.Key = strings.TrimPrefix(n1.Key, key)
				srvRecord.Key = strings.TrimSuffix(srvRecord.Key, srvRecord.Host)
				srvRecords = append(srvRecords, srvRecord)
			}
		}
	}
	if srvRecords != nil {
		sort.Slice(srvRecords, func(i int, j int) bool {
			return srvRecords[i].Key < srvRecords[j].Key
		})
	} else {
		return nil, ErrNoEntriesFound
	}
	return srvRecords, nil
}

// Adds DNS entries into etcd endpoint in CoreDNS etcd message format.
func (c *coreDNS) Put(bucket string) error {
	for ip := range c.domainIPs {
		bucketMsg, err := newCoreDNSMsg(bucket, ip, c.domainPort, defaultTTL)
		if err != nil {
			return err
		}
		kapi := etcd.NewKeysAPI(c.etcdClient)
		key := msg.Path(fmt.Sprintf("%s.%s", bucket, c.domainName), defaultPrefixPath)
		key = key + "/" + ip
		ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
		_, err = kapi.Set(ctx, key, string(bucketMsg), nil)
		cancel()
		if err != nil {
			ctx, cancel = context.WithTimeout(context.Background(), defaultContextTimeout)
			kapi.Delete(ctx, key, &etcd.DeleteOptions{Recursive: true})
			cancel()
			return err
		}
	}
	return nil
}

// Removes DNS entries added in Put().
func (c *coreDNS) Delete(bucket string) error {
	kapi := etcd.NewKeysAPI(c.etcdClient)
	key := msg.Path(fmt.Sprintf("%s.%s.", bucket, c.domainName), defaultPrefixPath)
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	_, err := kapi.Delete(ctx, key, &etcd.DeleteOptions{Recursive: true})
	cancel()
	return err
}

// CoreDNS - represents dns config for coredns server.
type coreDNS struct {
	domainName string
	domainIPs  set.StringSet
	domainPort int
	etcdClient etcd.Client
}

// NewCoreDNS - initialize a new coreDNS set/unset values.
func NewCoreDNS(domainName string, domainIPs set.StringSet, domainPort string, etcdClient etcd.Client) (Config, error) {
	if domainName == "" || domainIPs.IsEmpty() || etcdClient == nil {
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
