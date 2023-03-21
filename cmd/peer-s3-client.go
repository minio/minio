// Copyright (c) 2015-2022 MinIO, Inc.
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
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/rest"
	"github.com/minio/minio/internal/sync/errgroup"
	xnet "github.com/minio/pkg/net"
)

var errPeerOffline = errors.New("peer is offline")

// client to talk to peer Nodes.
type peerS3Client struct {
	host       *xnet.Host
	restClient *rest.Client
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerS3Client) call(method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	return client.callWithContext(GlobalContext, method, values, body, length)
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *peerS3Client) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	if values == nil {
		values = make(url.Values)
	}

	respBody, err = client.restClient.Call(ctx, method, values, body, length)
	if err == nil {
		return respBody, nil
	}

	err = toStorageErr(err)
	return nil, err
}

// S3PeerSys - S3 peer call system.
type S3PeerSys struct {
	peerClients    []*peerS3Client // Excludes self
	allPeerClients []*peerS3Client // Includes nil client for self
}

// NewS3PeerSys - creates new S3 peer calls.
func NewS3PeerSys(endpoints EndpointServerPools) *S3PeerSys {
	remote, all := newPeerS3Clients(endpoints)
	return &S3PeerSys{
		peerClients:    remote,
		allPeerClients: all,
	}
}

// ListBuckets lists buckets across all servers and returns a possible consistent view
func (sys *S3PeerSys) ListBuckets(ctx context.Context, opts BucketOptions) (result []BucketInfo, err error) {
	g := errgroup.WithNErrs(len(sys.peerClients))

	localBuckets, err := listBucketsLocal(ctx, opts)
	if err != nil {
		return nil, err
	}

	nodeBuckets := make([][]BucketInfo, len(sys.peerClients)+1)
	errs := []error{nil}
	nodeBuckets[0] = localBuckets

	for idx, client := range sys.peerClients {
		idx := idx
		client := client
		g.Go(func() error {
			if client == nil {
				return errPeerOffline
			}
			localBuckets, err := client.ListBuckets(ctx, opts)
			if err != nil {
				return err
			}
			nodeBuckets[idx+1] = localBuckets
			return nil
		}, idx)
	}

	errs = append(errs, g.Wait()...)

	quorum := (len(sys.allPeerClients) / 2)
	if err = reduceReadQuorumErrs(ctx, errs, bucketOpIgnoredErrs, quorum); err != nil {
		return nil, err
	}

	bucketsMap := make(map[string]struct{})
	for idx, buckets := range nodeBuckets {
		if errs[idx] != nil {
			continue
		}
		for _, bi := range buckets {
			_, ok := bucketsMap[bi.Name]
			if !ok {
				bucketsMap[bi.Name] = struct{}{}
				result = append(result, bi)
			}
		}
	}

	return result, nil
}

// GetBucketInfo returns bucket stat info about bucket on disk across all peers
func (sys *S3PeerSys) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (binfo BucketInfo, err error) {
	g := errgroup.WithNErrs(len(sys.peerClients))

	bucketInfos := make([]BucketInfo, len(sys.peerClients)+1)

	bucketInfo, err := getBucketInfoLocal(ctx, bucket, opts)
	if err != nil {
		return BucketInfo{}, err
	}

	errs := []error{nil}
	bucketInfos[0] = bucketInfo

	for idx, client := range sys.peerClients {
		idx := idx
		client := client
		g.Go(func() error {
			if client == nil {
				return errPeerOffline
			}
			bucketInfo, err := client.GetBucketInfo(ctx, bucket, opts)
			if err != nil {
				return err
			}
			bucketInfos[idx+1] = bucketInfo
			return nil
		}, idx)
	}

	errs = append(errs, g.Wait()...)

	quorum := (len(sys.allPeerClients) / 2)
	if err = reduceReadQuorumErrs(ctx, errs, bucketOpIgnoredErrs, quorum); err != nil {
		return BucketInfo{}, toObjectErr(err, bucket)
	}

	for i, err := range errs {
		if err == nil {
			bucketInfo = bucketInfos[i]
			break
		}
	}

	return bucketInfo, nil
}

func (client *peerS3Client) ListBuckets(ctx context.Context, opts BucketOptions) ([]BucketInfo, error) {
	v := url.Values{}
	v.Set(peerS3BucketDeleted, strconv.FormatBool(opts.Deleted))

	respBody, err := client.call(peerS3MethodListBuckets, v, nil, -1)
	if err != nil {
		return nil, err
	}
	defer xhttp.DrainBody(respBody)

	var buckets []BucketInfo
	err = gob.NewDecoder(respBody).Decode(&buckets)
	return buckets, err
}

// GetBucketInfo returns bucket stat info from a peer
func (client *peerS3Client) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (BucketInfo, error) {
	v := url.Values{}
	v.Set(peerS3Bucket, bucket)
	v.Set(peerS3BucketDeleted, strconv.FormatBool(opts.Deleted))

	respBody, err := client.call(peerS3MethodGetBucketInfo, v, nil, -1)
	if err != nil {
		return BucketInfo{}, err
	}
	defer xhttp.DrainBody(respBody)

	var bucketInfo BucketInfo
	err = gob.NewDecoder(respBody).Decode(&bucketInfo)
	return bucketInfo, err
}

// MakeBucket creates bucket across all peers
func (sys *S3PeerSys) MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	g := errgroup.WithNErrs(len(sys.peerClients))

	for idx, client := range sys.peerClients {
		client := client
		g.Go(func() error {
			if client == nil {
				return errPeerOffline
			}
			return client.MakeBucket(ctx, bucket, opts)
		}, idx)
	}

	errs := g.Wait()
	errs = append(errs, makeBucketLocal(ctx, bucket, opts))

	quorum := (len(sys.allPeerClients) / 2) + 1
	err := reduceWriteQuorumErrs(ctx, errs, bucketOpIgnoredErrs, quorum)
	return toObjectErr(err, bucket)
}

// MakeBucket creates a bucket on a peer
func (client *peerS3Client) MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	v := url.Values{}
	v.Set(peerS3Bucket, bucket)
	v.Set(peerS3BucketForceCreate, strconv.FormatBool(opts.ForceCreate))

	respBody, err := client.call(peerS3MethodMakeBucket, v, nil, -1)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)

	return nil
}

// DeleteBucket deletes bucket across all peers
func (sys *S3PeerSys) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	g := errgroup.WithNErrs(len(sys.peerClients))

	for idx, client := range sys.peerClients {
		client := client
		g.Go(func() error {
			if client == nil {
				return errPeerOffline
			}
			return client.DeleteBucket(ctx, bucket, opts)
		}, idx)
	}

	errs := g.Wait()
	errs = append(errs, deleteBucketLocal(ctx, bucket, opts))

	quorum := (len(sys.allPeerClients) / 2) + 1
	err := reduceWriteQuorumErrs(ctx, errs, bucketOpIgnoredErrs, quorum)
	return toObjectErr(err, bucket)
}

// DeleteBucket deletes bucket on a peer
func (client *peerS3Client) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	v := url.Values{}
	v.Set(peerS3Bucket, bucket)
	v.Set(peerS3BucketForceDelete, strconv.FormatBool(opts.Force))

	respBody, err := client.call(peerS3MethodDeleteBucket, v, nil, -1)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(respBody)

	return nil
}

// newPeerS3Clients creates new peer clients.
// The two slices will point to the same clients,
// but 'all' will contain nil entry for local client.
// The 'all' slice will be in the same order across the cluster.
func newPeerS3Clients(endpoints EndpointServerPools) (remote, all []*peerS3Client) {
	if !globalIsDistErasure {
		// Only useful in distributed setups
		return nil, nil
	}
	hosts := endpoints.hostsSorted()
	remote = make([]*peerS3Client, 0, len(hosts))
	all = make([]*peerS3Client, len(hosts))
	for i, host := range hosts {
		if host == nil {
			continue
		}
		all[i] = newPeerS3Client(host)
		remote = append(remote, all[i])
	}
	if len(all) != len(remote)+1 {
		logger.LogIf(context.Background(), fmt.Errorf("WARNING: Expected number of all hosts (%v) to be remote +1 (%v)", len(all), len(remote)))
	}
	return remote, all
}

// Returns a peer S3 client.
func newPeerS3Client(peer *xnet.Host) *peerS3Client {
	scheme := "http"
	if globalIsTLS {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   peer.String(),
		Path:   peerS3Path,
	}

	restClient := rest.NewClient(serverURL, globalInternodeTransport, newCachedAuthToken())
	// Use a separate client to avoid recursive calls.
	healthClient := rest.NewClient(serverURL, globalInternodeTransport, newCachedAuthToken())
	healthClient.ExpectTimeouts = true
	healthClient.NoMetrics = true

	// Construct a new health function.
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), restClient.HealthCheckTimeout)
		defer cancel()
		respBody, err := healthClient.Call(ctx, peerS3MethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		return !isNetworkError(err)
	}

	return &peerS3Client{host: peer, restClient: restClient}
}
