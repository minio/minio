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

package cmd

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// AnonGetBucketInfo - Get bucket metadata from azure anonymously.
func (a AzureObjects) AnonGetBucketInfo(bucket string) (bucketInfo BucketInfo, err error) {
	url, err := url.Parse(a.client.GetBlobURL(bucket, ""))
	if err != nil {
		return bucketInfo, azureToObjectError(traceError(err))
	}
	url.RawQuery = "restype=container"
	resp, err := http.Head(url.String())
	if err != nil {
		return bucketInfo, azureToObjectError(traceError(err), bucket)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return bucketInfo, azureToObjectError(traceError(anonErrToObjectErr(resp.StatusCode, bucket)), bucket)
	}

	t, err := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))
	if err != nil {
		return bucketInfo, traceError(err)
	}
	bucketInfo = BucketInfo{
		Name:    bucket,
		Created: t,
	}
	return bucketInfo, nil
}

// AnonGetObject - SendGET request without authentication.
// This is needed when clients send GET requests on objects that can be downloaded without auth.
func (a AzureObjects) AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	url := a.client.GetBlobURL(bucket, object)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return azureToObjectError(traceError(err), bucket, object)
	}

	if length > 0 && startOffset > 0 {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", startOffset, startOffset+length-1))
	} else if startOffset > 0 {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-", startOffset))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return azureToObjectError(traceError(err), bucket, object)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return azureToObjectError(traceError(anonErrToObjectErr(resp.StatusCode, bucket, object)), bucket, object)
	}

	_, err = io.Copy(writer, resp.Body)
	return traceError(err)
}

// AnonGetObjectInfo - Send HEAD request without authentication and convert the
// result to ObjectInfo.
func (a AzureObjects) AnonGetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error) {
	resp, err := http.Head(a.client.GetBlobURL(bucket, object))
	if err != nil {
		return objInfo, azureToObjectError(traceError(err), bucket, object)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return objInfo, azureToObjectError(traceError(anonErrToObjectErr(resp.StatusCode, bucket, object)), bucket, object)
	}

	var contentLength int64
	contentLengthStr := resp.Header.Get("Content-Length")
	if contentLengthStr != "" {
		contentLength, err = strconv.ParseInt(contentLengthStr, 0, 64)
		if err != nil {
			return objInfo, azureToObjectError(traceError(errUnexpected), bucket, object)
		}
	}

	t, err := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))
	if err != nil {
		return objInfo, traceError(err)
	}

	objInfo.ModTime = t
	objInfo.Bucket = bucket
	objInfo.UserDefined = make(map[string]string)
	if resp.Header.Get("Content-Encoding") != "" {
		objInfo.UserDefined["Content-Encoding"] = resp.Header.Get("Content-Encoding")
	}
	objInfo.UserDefined["Content-Type"] = resp.Header.Get("Content-Type")
	objInfo.MD5Sum = resp.Header.Get("Etag")
	objInfo.ModTime = t
	objInfo.Name = object
	objInfo.Size = contentLength
	return
}

// AnonListObjects - Use Azure equivalent ListBlobs.
func (a AzureObjects) AnonListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	params := storage.ListBlobsParameters{
		Prefix:     prefix,
		Marker:     marker,
		Delimiter:  delimiter,
		MaxResults: uint(maxKeys),
	}

	q := azureListBlobsGetParameters(params)
	q.Set("restype", "container")
	q.Set("comp", "list")

	url, err := url.Parse(a.client.GetBlobURL(bucket, ""))
	if err != nil {
		return result, azureToObjectError(traceError(err))
	}
	url.RawQuery = q.Encode()

	resp, err := http.Get(url.String())
	if err != nil {
		return result, azureToObjectError(traceError(err))
	}
	defer resp.Body.Close()

	var listResp storage.BlobListResponse

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, azureToObjectError(traceError(err))
	}
	err = xml.Unmarshal(data, &listResp)
	if err != nil {
		return result, azureToObjectError(traceError(err))
	}

	result.IsTruncated = listResp.NextMarker != ""
	result.NextMarker = listResp.NextMarker
	for _, object := range listResp.Blobs {
		t, e := time.Parse(time.RFC1123, object.Properties.LastModified)
		if e != nil {
			continue
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Bucket:          bucket,
			Name:            object.Name,
			ModTime:         t,
			Size:            object.Properties.ContentLength,
			MD5Sum:          object.Properties.Etag,
			ContentType:     object.Properties.ContentType,
			ContentEncoding: object.Properties.ContentEncoding,
		})
	}
	result.Prefixes = listResp.BlobPrefixes
	return result, nil
}
