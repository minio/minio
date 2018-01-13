/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package azure

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/minio/minio/pkg/errors"

	minio "github.com/minio/minio/cmd"
)

// Copied from github.com/Azure/azure-sdk-for-go/storage/container.go
func azureListBlobsGetParameters(p storage.ListBlobsParameters) url.Values {
	out := url.Values{}

	if p.Prefix != "" {
		out.Set("prefix", p.Prefix)
	}
	if p.Delimiter != "" {
		out.Set("delimiter", p.Delimiter)
	}
	if p.Marker != "" {
		out.Set("marker", p.Marker)
	}
	if p.Include != nil {
		addString := func(datasets []string, include bool, text string) []string {
			if include {
				datasets = append(datasets, text)
			}
			return datasets
		}

		include := []string{}
		include = addString(include, p.Include.Snapshots, "snapshots")
		include = addString(include, p.Include.Metadata, "metadata")
		include = addString(include, p.Include.UncommittedBlobs, "uncommittedblobs")
		include = addString(include, p.Include.Copy, "copy")
		fullInclude := strings.Join(include, ",")
		out.Set("include", fullInclude)
	}
	if p.MaxResults != 0 {
		out.Set("maxresults", fmt.Sprintf("%v", p.MaxResults))
	}
	if p.Timeout != 0 {
		out.Set("timeout", fmt.Sprintf("%v", p.Timeout))
	}

	return out
}

// Make anonymous HTTP request to azure endpoint.
func azureAnonRequest(verb, urlStr string, header http.Header) (*http.Response, error) {
	req, err := http.NewRequest(verb, urlStr, nil)
	if err != nil {
		return nil, err
	}
	if header != nil {
		req.Header = header
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	// 4XX and 5XX are error HTTP codes.
	if resp.StatusCode >= 400 && resp.StatusCode <= 511 {
		defer resp.Body.Close()
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		if len(respBody) == 0 {
			// no error in response body, might happen in HEAD requests
			return nil, storage.AzureStorageServiceError{
				StatusCode: resp.StatusCode,
				Code:       resp.Status,
				Message:    "no response body was available for error status code",
			}
		}
		// Response contains Azure storage service error object.
		var storageErr storage.AzureStorageServiceError
		if err := xml.Unmarshal(respBody, &storageErr); err != nil {
			return nil, err
		}
		storageErr.StatusCode = resp.StatusCode
		return nil, storageErr
	}

	return resp, nil
}

// AnonGetBucketInfo - Get bucket metadata from azure anonymously.
func (a *azureObjects) AnonGetBucketInfo(bucket string) (bucketInfo minio.BucketInfo, err error) {
	blobURL := a.client.GetContainerReference(bucket).GetBlobReference("").GetURL()
	url, err := url.Parse(blobURL)
	if err != nil {
		return bucketInfo, azureToObjectError(errors.Trace(err))
	}
	url.RawQuery = "restype=container"
	resp, err := azureAnonRequest(http.MethodHead, url.String(), nil)
	if err != nil {
		return bucketInfo, azureToObjectError(errors.Trace(err), bucket)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return bucketInfo, azureToObjectError(errors.Trace(minio.AnonErrToObjectErr(resp.StatusCode, bucket)), bucket)
	}

	t, err := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))
	if err != nil {
		return bucketInfo, errors.Trace(err)
	}

	return minio.BucketInfo{
		Name:    bucket,
		Created: t,
	}, nil
}

// AnonGetObject - SendGET request without authentication.
// This is needed when clients send GET requests on objects that can be downloaded without auth.
func (a *azureObjects) AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer, etag string) (err error) {
	h := make(http.Header)
	if length > 0 && startOffset > 0 {
		h.Add("Range", fmt.Sprintf("bytes=%d-%d", startOffset, startOffset+length-1))
	} else if startOffset > 0 {
		h.Add("Range", fmt.Sprintf("bytes=%d-", startOffset))
	}

	blobURL := a.client.GetContainerReference(bucket).GetBlobReference(object).GetURL()
	resp, err := azureAnonRequest(http.MethodGet, blobURL, h)
	if err != nil {
		return azureToObjectError(errors.Trace(err), bucket, object)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return azureToObjectError(errors.Trace(minio.AnonErrToObjectErr(resp.StatusCode, bucket, object)), bucket, object)
	}

	_, err = io.Copy(writer, resp.Body)
	return errors.Trace(err)
}

// AnonGetObjectInfo - Send HEAD request without authentication and convert the
// result to ObjectInfo.
func (a *azureObjects) AnonGetObjectInfo(bucket, object string) (objInfo minio.ObjectInfo, err error) {
	blobURL := a.client.GetContainerReference(bucket).GetBlobReference(object).GetURL()
	resp, err := azureAnonRequest(http.MethodHead, blobURL, nil)
	if err != nil {
		return objInfo, azureToObjectError(errors.Trace(err), bucket, object)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return objInfo, azureToObjectError(errors.Trace(minio.AnonErrToObjectErr(resp.StatusCode, bucket, object)), bucket, object)
	}

	var contentLength int64
	contentLengthStr := resp.Header.Get("Content-Length")
	if contentLengthStr != "" {
		contentLength, err = strconv.ParseInt(contentLengthStr, 0, 64)
		if err != nil {
			return objInfo, azureToObjectError(errors.Trace(fmt.Errorf("Unexpected error")), bucket, object)
		}
	}

	t, err := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))
	if err != nil {
		return objInfo, errors.Trace(err)
	}

	objInfo.ModTime = t
	objInfo.Bucket = bucket
	objInfo.UserDefined = make(map[string]string)
	if resp.Header.Get("Content-Encoding") != "" {
		objInfo.UserDefined["Content-Encoding"] = resp.Header.Get("Content-Encoding")
	}
	objInfo.UserDefined["Content-Type"] = resp.Header.Get("Content-Type")
	objInfo.ETag = resp.Header.Get("Etag")
	objInfo.ModTime = t
	objInfo.Name = object
	objInfo.Size = contentLength
	return
}

// AnonListObjects - Use Azure equivalent ListBlobs.
func (a *azureObjects) AnonListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	params := storage.ListBlobsParameters{
		Prefix:     prefix,
		Marker:     marker,
		Delimiter:  delimiter,
		MaxResults: uint(maxKeys),
	}

	q := azureListBlobsGetParameters(params)
	q.Set("restype", "container")
	q.Set("comp", "list")

	blobURL := a.client.GetContainerReference(bucket).GetBlobReference("").GetURL()
	url, err := url.Parse(blobURL)
	if err != nil {
		return result, azureToObjectError(errors.Trace(err))
	}
	url.RawQuery = q.Encode()

	resp, err := azureAnonRequest(http.MethodGet, url.String(), nil)
	if err != nil {
		return result, azureToObjectError(errors.Trace(err))
	}
	defer resp.Body.Close()

	var listResp storage.BlobListResponse

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, azureToObjectError(errors.Trace(err))
	}
	err = xml.Unmarshal(data, &listResp)
	if err != nil {
		return result, azureToObjectError(errors.Trace(err))
	}

	result.IsTruncated = listResp.NextMarker != ""
	result.NextMarker = listResp.NextMarker
	for _, object := range listResp.Blobs {
		result.Objects = append(result.Objects, minio.ObjectInfo{
			Bucket:          bucket,
			Name:            object.Name,
			ModTime:         time.Time(object.Properties.LastModified),
			Size:            object.Properties.ContentLength,
			ETag:            object.Properties.Etag,
			ContentType:     object.Properties.ContentType,
			ContentEncoding: object.Properties.ContentEncoding,
		})
	}
	result.Prefixes = listResp.BlobPrefixes
	return result, nil
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (a *azureObjects) AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result minio.ListObjectsV2Info, err error) {
	params := storage.ListBlobsParameters{
		Prefix:     prefix,
		Marker:     continuationToken,
		Delimiter:  delimiter,
		MaxResults: uint(maxKeys),
	}

	q := azureListBlobsGetParameters(params)
	q.Set("restype", "container")
	q.Set("comp", "list")

	blobURL := a.client.GetContainerReference(bucket).GetBlobReference("").GetURL()
	url, err := url.Parse(blobURL)
	if err != nil {
		return result, azureToObjectError(errors.Trace(err))
	}
	url.RawQuery = q.Encode()

	resp, err := http.Get(url.String())
	if err != nil {
		return result, azureToObjectError(errors.Trace(err))
	}
	defer resp.Body.Close()

	var listResp storage.BlobListResponse

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, azureToObjectError(errors.Trace(err))
	}
	err = xml.Unmarshal(data, &listResp)
	if err != nil {
		return result, azureToObjectError(errors.Trace(err))
	}

	// If NextMarker is not empty, this means response is truncated and NextContinuationToken should be set
	if listResp.NextMarker != "" {
		result.IsTruncated = true
		result.NextContinuationToken = listResp.NextMarker
	}
	for _, object := range listResp.Blobs {
		result.Objects = append(result.Objects, minio.ObjectInfo{
			Bucket:          bucket,
			Name:            object.Name,
			ModTime:         time.Time(object.Properties.LastModified),
			Size:            object.Properties.ContentLength,
			ETag:            minio.CanonicalizeETag(object.Properties.Etag),
			ContentType:     object.Properties.ContentType,
			ContentEncoding: object.Properties.ContentEncoding,
		})
	}
	result.Prefixes = listResp.BlobPrefixes
	return result, nil
}
