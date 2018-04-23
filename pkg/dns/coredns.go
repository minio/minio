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
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coredns/coredns/plugin/etcd/msg"
	etcd "github.com/coreos/etcd/client"
)

// create a new coredns service record for the bucket.
func newCoreDNSMsg(bucket string, ip string, port int, ttl uint32) ([]byte, error) {
	return json.Marshal(&SrvRecord{
		Host:         ip,
		Port:         port,
		TTL:          ttl,
		CreationDate: time.Now().UTC(),
	})
}

// Retrieves list of DNS entries for a bucket.
func (c *coreDNS) List() ([]SrvRecord, error) {
	kapi := etcd.NewKeysAPI(c.etcdClient)
	key := msg.Path(fmt.Sprintf("%s.", c.domainName), defaultPrefixPath)
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	r, err := kapi.Get(ctx, key, nil)
	cancel()
	if err != nil {
		return nil, err
	}

	var srvRecords []SrvRecord
	for _, n := range r.Node.Nodes {
		var srvRecord SrvRecord
		if err = json.Unmarshal([]byte(n.Value), &srvRecord); err != nil {
			return nil, err
		}
		srvRecord.Key = strings.TrimPrefix(n.Key, key)
		srvRecords = append(srvRecords, srvRecord)
	}
	sort.Slice(srvRecords, func(i int, j int) bool {
		return srvRecords[i].Key < srvRecords[j].Key
	})
	return srvRecords, nil
}

// Retrieves DNS record for a bucket.
func (c *coreDNS) Get(bucket string) (SrvRecord, error) {
	kapi := etcd.NewKeysAPI(c.etcdClient)
	key := msg.Path(fmt.Sprintf("%s.%s.", bucket, c.domainName), defaultPrefixPath)
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	r, err := kapi.Get(ctx, key, nil)
	cancel()
	if err != nil {
		return SrvRecord{}, err
	}
	var sr SrvRecord
	if err = json.Unmarshal([]byte(r.Node.Value), &sr); err != nil {
		return SrvRecord{}, err
	}
	sr.Key = strings.TrimPrefix(r.Node.Key, key)
	return sr, nil
}

// Adds DNS entries into etcd endpoint in CoreDNS etcd messae format.
func (c *coreDNS) Put(bucket string) error {
	bucketMsg, err := newCoreDNSMsg(bucket, c.domainIP, c.domainPort, defaultTTL)
	if err != nil {
		return err
	}
	kapi := etcd.NewKeysAPI(c.etcdClient)
	key := msg.Path(fmt.Sprintf("%s.%s.", bucket, c.domainName), defaultPrefixPath)
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	_, err = kapi.Set(ctx, key, string(bucketMsg), nil)
	cancel()
	return err
}

// Removes DNS entries added in Put().
func (c *coreDNS) Delete(bucket string) error {
	kapi := etcd.NewKeysAPI(c.etcdClient)
	key := msg.Path(fmt.Sprintf("%s.%s.", bucket, c.domainName), defaultPrefixPath)
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	_, err := kapi.Delete(ctx, key, nil)
	cancel()
	return err
}

// CoreDNS - represents dns config for coredns server.
type coreDNS struct {
	domainName, domainIP string
	domainPort           int
	etcdClient           etcd.Client
}

// NewCoreDNS - initialize a new coreDNS set/unset values.
func NewCoreDNS(domainName, domainIP, domainPort string, etcdClient etcd.Client) (Config, error) {
	if domainName == "" || domainIP == "" || etcdClient == nil {
		return nil, errors.New("invalid argument")
	}

	if net.ParseIP(domainIP) == nil {
		return nil, errors.New("invalid argument")
	}

	port, err := strconv.Atoi(domainPort)
	if err != nil {
		return nil, err
	}

	return &coreDNS{
		domainName: domainName,
		domainIP:   domainIP,
		domainPort: port,
		etcdClient: etcdClient,
	}, nil
}
