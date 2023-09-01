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
	"io"
	"net/url"
	"strconv"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/rest"
	"github.com/minio/pkg/v2/sync/errgroup"
	"golang.org/x/exp/slices"
)

var errPeerOffline = errors.New("peer is offline")

type peerS3Client interface {
	ListBuckets(ctx context.Context, opts BucketOptions) ([]BucketInfo, error)
	GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (BucketInfo, error)
	MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error
	DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error

	GetHost() string
	SetPools([]int)
	GetPools() []int
}

type localPeerS3Client struct {
	host  string
	pools []int
}

func (l *localPeerS3Client) GetHost() string {
	return l.host
}

func (l *localPeerS3Client) SetPools(p []int) {
	l.pools = make([]int, len(p))
	copy(l.pools, p)
}

func (l localPeerS3Client) GetPools() []int {
	return l.pools
}

func (l localPeerS3Client) ListBuckets(ctx context.Context, opts BucketOptions) ([]BucketInfo, error) {
	return listBucketsLocal(ctx, opts)
}

func (l localPeerS3Client) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (BucketInfo, error) {
	return getBucketInfoLocal(ctx, bucket, opts)
}

func (l localPeerS3Client) MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	return makeBucketLocal(ctx, bucket, opts)
}

func (l localPeerS3Client) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	return deleteBucketLocal(ctx, bucket, opts)
}

// client to talk to peer Nodes.
type remotePeerS3Client struct {
	host       string
	pools      []int
	restClient *rest.Client
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *remotePeerS3Client) call(method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
	return client.callWithContext(GlobalContext, method, values, body, length)
}

// Wrapper to restClient.Call to handle network errors, in case of network error the connection is marked disconnected
// permanently. The only way to restore the connection is at the xl-sets layer by xlsets.monitorAndConnectEndpoints()
// after verifying format.json
func (client *remotePeerS3Client) callWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (respBody io.ReadCloser, err error) {
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
	peerClients []peerS3Client // Excludes self
	poolsCount  int
}

// NewS3PeerSys - creates new S3 peer calls.
func NewS3PeerSys(endpoints EndpointServerPools) *S3PeerSys {
	return &S3PeerSys{
		peerClients: newPeerS3Clients(endpoints.GetNodes()),
		poolsCount:  len(endpoints),
	}
}

// ListBuckets lists buckets across all servers and returns a possible consistent view
func (sys *S3PeerSys) ListBuckets(ctx context.Context, opts BucketOptions) (result []BucketInfo, err error) {
	g := errgroup.WithNErrs(len(sys.peerClients))

	nodeBuckets := make([][]BucketInfo, len(sys.peerClients))
	errs := []error{nil}

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
			nodeBuckets[idx] = localBuckets
			return nil
		}, idx)
	}

	errs = append(errs, g.Wait()...)

	quorum := len(sys.peerClients)/2 + 1
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

	bucketInfos := make([]BucketInfo, len(sys.peerClients))
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
			bucketInfos[idx] = bucketInfo
			return nil
		}, idx)
	}

	errs := g.Wait()

	quorum := len(sys.peerClients)/2 + 1
	if err = reduceReadQuorumErrs(ctx, errs, bucketOpIgnoredErrs, quorum); err != nil {
		return BucketInfo{}, toObjectErr(err, bucket)
	}

	for i, err := range errs {
		if err == nil {
			return bucketInfos[i], nil
		}
	}

	return BucketInfo{}, toObjectErr(errVolumeNotFound, bucket)
}

func (client *remotePeerS3Client) ListBuckets(ctx context.Context, opts BucketOptions) ([]BucketInfo, error) {
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
func (client *remotePeerS3Client) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (BucketInfo, error) {
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

	for poolIdx := 0; poolIdx < sys.poolsCount; poolIdx++ {
		perPoolErrs := make([]error, 0, len(sys.peerClients))
		for i, client := range sys.peerClients {
			if slices.Contains(client.GetPools(), poolIdx) {
				perPoolErrs = append(perPoolErrs, errs[i])
			}
		}
		if poolErr := reduceWriteQuorumErrs(ctx, perPoolErrs, bucketOpIgnoredErrs, len(perPoolErrs)/2+1); poolErr != nil {
			return toObjectErr(poolErr, bucket)
		}
	}
	return nil
}

// MakeBucket creates a bucket on a peer
func (client *remotePeerS3Client) MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error {
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

	for poolIdx := 0; poolIdx < sys.poolsCount; poolIdx++ {
		perPoolErrs := make([]error, 0, len(sys.peerClients))
		for i, client := range sys.peerClients {
			if slices.Contains(client.GetPools(), poolIdx) {
				perPoolErrs = append(perPoolErrs, errs[i])
			}
		}
		if poolErr := reduceWriteQuorumErrs(ctx, perPoolErrs, bucketOpIgnoredErrs, len(perPoolErrs)/2+1); poolErr != nil && poolErr != errVolumeNotFound {
			// re-create successful deletes, since we are return an error.
			sys.MakeBucket(ctx, bucket, MakeBucketOptions{})
			return toObjectErr(poolErr, bucket)
		}
	}
	return nil
}

// DeleteBucket deletes bucket on a peer
func (client *remotePeerS3Client) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
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

func (client remotePeerS3Client) GetHost() string {
	return client.host
}

func (client remotePeerS3Client) GetPools() []int {
	return client.pools
}

func (client *remotePeerS3Client) SetPools(p []int) {
	client.pools = make([]int, len(p))
	copy(client.pools, p)
}

// newPeerS3Clients creates new peer clients.
func newPeerS3Clients(nodes []Node) (peers []peerS3Client) {
	peers = make([]peerS3Client, len(nodes))
	for i, node := range nodes {
		if node.IsLocal {
			peers[i] = &localPeerS3Client{host: node.Host}
		} else {
			peers[i] = newPeerS3Client(node.Host)
		}
		peers[i].SetPools(node.Pools)
	}
	return
}

// Returns a peer S3 client.
func newPeerS3Client(peer string) peerS3Client {
	scheme := "http"
	if globalIsTLS {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   peer,
		Path:   peerS3Path,
	}

	restClient := rest.NewClient(serverURL, globalInternodeTransport, newCachedAuthToken())
	// Use a separate client to avoid recursive calls.
	healthClient := rest.NewClient(serverURL, globalInternodeTransport, newCachedAuthToken())
	healthClient.NoMetrics = true

	// Construct a new health function.
	restClient.HealthCheckFn = func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), restClient.HealthCheckTimeout)
		defer cancel()
		respBody, err := healthClient.Call(ctx, peerS3MethodHealth, nil, nil, -1)
		xhttp.DrainBody(respBody)
		return !isNetworkError(err)
	}

	return &remotePeerS3Client{host: peer, restClient: restClient}
}
