package cmd

import (
	"io"
	"fmt"
	"errors"
	"encoding/json"
	"net/http"
	"path/filepath"
	"sort"
	"bufio"
	"net"
	"os"
	"github.com/bgentry/speakeasy"

	"github.com/minio/minio-go/pkg/policy"
	"github.com/NebulousLabs/Sia/modules"
	"github.com/NebulousLabs/Sia/api"
)

var (
	addr = "127.0.0.1:9980"
	siaBucketPath = ".sia_buckets"
	siaCachePath = ".sia_cache"

	// User-supplied password, cached so that we don't need to prompt multiple
	// times.
	apiPassword string
)

func init() {
	os.Mkdir(siaBucketPath, 0744)
	os.Mkdir(siaCachePath, 0744)
} 


type siaObjects struct {
	//client   sia.Client
}

func copyFile(in io.Reader, dst string) (err error) {

    // Does file already exist?
    if _, err := os.Stat(dst); err == nil {
        return nil
    }

    err = nil

    out, err := os.Create(dst)
    if err != nil {
        return err
    }

    defer func() {
        cerr := out.Close()
        if err == nil {
            err = cerr
        }
    }()


    
    _, err = io.Copy(out, in)
    if err != nil {
        return err
    }

    err = out.Sync()
    return
}

// newSiaGateway returns Sia gatewaylayer
func newSiaGateway(host string) (GatewayLayer, error) {

	return &siaObjects{
		//Client:     client,
	}, nil
}

// bySiaPath implements sort.Interface for [] modules.FileInfo based on the
// SiaPath field.
type bySiaPath []modules.FileInfo

func (s bySiaPath) Len() int           { return len(s) }
func (s bySiaPath) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s bySiaPath) Less(i, j int) bool { return s[i].SiaPath < s[j].SiaPath }

// abs returns the absolute representation of a path.
// TODO: bad things can happen if you run siac from a non-existent directory.
// Implement some checks to catch this problem.
func abs(path string) string {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abspath
}

// non2xx returns true for non-success HTTP status codes.
func non2xx(code int) bool {
	return code < 200 || code > 299
}

// decodeError returns the api.Error from a API response. This method should
// only be called if the response's status code is non-2xx. The error returned
// may not be of type api.Error in the event of an error unmarshalling the
// JSON.
func decodeError(resp *http.Response) error {
	var apiErr api.Error
	err := json.NewDecoder(resp.Body).Decode(&apiErr)
	if err != nil {
		return err
	}
	return apiErr
}

// apiGet wraps a GET request with a status code check, such that if the GET does
// not return 2xx, the error will be read and returned. The response body is
// not closed.
func apiGet(call string) (*http.Response, error) {
	if host, port, _ := net.SplitHostPort(addr); host == "" {
		addr = net.JoinHostPort("localhost", port)
	}
	resp, err := api.HttpGET("http://" + addr + call)
	if err != nil {
		return nil, errors.New("no response from daemon")
	}
	// check error code
	if resp.StatusCode == http.StatusUnauthorized {
		// retry request with authentication.
		resp.Body.Close()
		if apiPassword == "" {
			// prompt for password and store it in a global var for subsequent
			// calls
			apiPassword, err = speakeasy.Ask("API password: ")
			if err != nil {
				return nil, err
			}
		}
		resp, err = api.HttpGETAuthenticated("http://"+addr+call, apiPassword)
		if err != nil {
			return nil, errors.New("no response from daemon - authentication failed")
		}
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errors.New("API call not recognized: " + call)
	}
	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// apiPost wraps a POST request with a status code check, such that if the POST
// does not return 2xx, the error will be read and returned. The response body
// is not closed.
func apiPost(call, vals string) (*http.Response, error) {
	
	if host, port, _ := net.SplitHostPort(addr); host == "" {
		addr = net.JoinHostPort("localhost", port)
	}

	resp, err := api.HttpPOST("http://"+addr+call, vals)
	if err != nil {
		return nil, errors.New("no response from daemon")
	}
	// check error code
	if resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()
		// Prompt for password and retry request with authentication.
		password, err := speakeasy.Ask("API password: ")
		if err != nil {
			return nil, err
		}
		resp, err = api.HttpPOSTAuthenticated("http://"+addr+call, vals, password)
		if err != nil {
			return nil, errors.New("no response from daemon - authentication failed")
		}
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, errors.New("API call not recognized: " + call)
	}
	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// post makes an API call and discards the response. An error is returned if
// the response status is not 2xx.
func post(call, vals string) error {
	resp, err := apiPost(call, vals)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// getAPI makes a GET API call and decodes the response. An error is returned
// if the response status is not 2xx.
func getAPI(call string, obj interface{}) error {
	resp, err := apiGet(call)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return errors.New("expecting a response, but API returned status code 204 No Content")
	}

	err = json.NewDecoder(resp.Body).Decode(obj)
	if err != nil {
		return err
	}
	return nil
}

// get makes an API call and discards the response. An error is returned if the
// response status is not 2xx.
func get(call string) error {
	resp, err := apiGet(call)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func readLines(path string) ([]string, error) {
	var lines []string

	file, err := os.Open(path)
	if err != nil {
		// File doesn't exist. Create it.
		file, err = os.Create(path)
		if err != nil {
			return lines, err
		}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func appendStringToFile(path, text string) error {
      f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
      if err != nil {
              return err
      }
      defer f.Close()

      var text2 = text + "\n"

      _, err = f.WriteString(text2)
      if err != nil {
              return err
      }
      return nil
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *siaObjects) Shutdown() error {
	// TODO
	return nil
}

// StorageInfo is not relevant to Sia backend.
func (l *siaObjects) StorageInfo() (si StorageInfo) {
	return si
}

// MakeBucket creates a new container on Sia backend.
func (l *siaObjects) MakeBucketWithLocation(bucket, location string) error {
	//fmt.Printf("MakeBucketWithLocation: bucket: %s, loc: %s", bucket, location)

	var bucketsFile = filepath.Join(siaBucketPath, "buckets.txt")

	lines, err := readLines(bucketsFile)
	if err != nil {
		// File not made yet.
	}
	for _, line := range lines {
		// Determine if bucket already exists
		if line == bucket {
			return errors.New("Bucket already exists")
		}
	}

	// Append bucket to list
	return appendStringToFile(bucketsFile, bucket)
}

// GetBucketInfo gets bucket metadata..
func (l *siaObjects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	bi.Name = bucket
	//bi.Created = 
	return bi, nil
}

// ListBuckets lists all Sia buckets
func (l *siaObjects) ListBuckets() (buckets []BucketInfo, e error) {
	var bucketsFile = filepath.Join(siaBucketPath, "buckets.txt")
	lines, err := readLines(bucketsFile)
	if err != nil {
		// File not made yet.
	}
	
	for _, line := range lines {
		buckets = append(buckets, BucketInfo{
			Name:    line,
			//Created: t,
		})
	}

	return buckets, nil
}

// DeleteBucket deletes a bucket on Sia
func (l *siaObjects) DeleteBucket(bucket string) error {

	var bucketsFile = filepath.Join(siaBucketPath, "buckets.txt")

	lines, err := readLines(bucketsFile)
	if err != nil {
		// File not made yet.
		return err
	}

	// Delete the file and recreate it.
	os.Remove(bucketsFile)

	for _, line := range lines {
		// Determine if bucket already exists
		if line != bucket {
			// Inefficient, but in a hurry...
			err = appendStringToFile(bucketsFile, bucket)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// ListObjects lists all blobs in Sia bucket filtered by prefix
func (l *siaObjects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	//fmt.Printf("ListObjects bucket: %s, prefix: %s, marker: %s, delimiter: %s, maxKeys: %d\n", bucket, prefix, marker, delimiter, maxKeys)

	var rf api.RenterFiles
	err := getAPI("/renter/files", &rf)
	if err != nil {
		return loi, err
	}

	if len(rf.Files) == 0 {
		// No files uploaded
		return loi, nil 
	}

	//fmt.Println("Tracking", len(rf.Files), "files:")
	
	loi.IsTruncated = false
	loi.NextMarker = ""

	sort.Sort(bySiaPath(rf.Files))
	for _, file := range rf.Files {
		if file.Available {
			//fmt.Printf("File: %s - Size: %d\n", file.SiaPath, file.Filesize)
			loi.Objects = append(loi.Objects, ObjectInfo{
				Bucket:          bucket,
				Name:            file.SiaPath,
				//ModTime:         "",
				Size:            int64(file.Filesize),
				//ETag:            azureToS3ETag(object.Properties.Etag),
				//ContentType:     object.Properties.ContentType,
				//ContentEncoding: object.Properties.ContentEncoding,
			})
		}
	}

	//loi.Prefixes = resp.BlobPrefixes

	return loi, nil
}

// ListObjectsV2 lists all blobs in S3 bucket filtered by prefix
func (l *siaObjects) ListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (loi ListObjectsV2Info, e error) {
	fmt.Println("ListObjectsV2")
	return loi, nil
}

func (l *siaObjects) GetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	
	//fmt.Printf("GetObject bucket: %s, key: %s, start: %d, len: %d", bucket, key, startOffset,length)
	// First, determine if object lives in cache directory
	var siaObj = bucket + "/" + key

    if _, err := os.Stat(filepath.Join(siaCachePath,siaObj)); err == nil {
        // File already exists in cache. Provide from it to avoid Sia fees.
        //fmt.Println("Accessing from Sia Cache")
    	reader, err := os.Open(filepath.Join(siaCachePath,siaObj))
		if err != nil {
		 	return err
		}

		_, err = io.Copy(writer, reader)
    	if err != nil {
        	return err
    	}

    	return nil
    }

    // Not in cache. Download from Sia.
    //fmt.Println("Downloading from Sia")

    // Make sure bucket path exists
	os.Mkdir(filepath.Join(siaCachePath, bucket), 0744)

    var tmpPath = filepath.Join(siaCachePath, siaObj)
	err := get("/renter/download/" + siaObj + "?destination=" + abs(tmpPath))

	if err != nil {
		return err
	}

	//fmt.Println("Downloaded from Sia")

	reader, err := os.Open(abs(tmpPath))
    if err != nil {
        return err
    }

    _, err = io.Copy(writer, reader)
    if err != nil {
        return err
    }

	return nil
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (l *siaObjects) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	//fmt.Printf("GetObjectInfo bucket: %s, object: %s", bucket, object)

	var siaObj = bucket + "/" + object;

	var rf api.RenterFiles
	err = getAPI("/renter/files", &rf)
	if err != nil {
		return objInfo, errors.New("Object not found")
	}

	if len(rf.Files) == 0 {
		// No files uploaded
		return objInfo, errors.New("Object not found") 
	}

	var file_found = false;
	for _, file := range rf.Files {
		if file.SiaPath == siaObj {
			fmt.Printf("File: %s - Size: %d\n", file.SiaPath, file.Filesize)
			objInfo.Bucket = bucket
			objInfo.Name = file.SiaPath
			//objInfo.ModTime = ""
			objInfo.Size = int64(file.Filesize)
			file_found = true;
		}
	}

	if !file_found {
		return objInfo, errors.New("Object not found")
	}

	return objInfo, nil
}

// PutObject creates a new object with the incoming data,
func (l *siaObjects) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (objInfo ObjectInfo, e error) {
	//fmt.Printf("PutObject: %s - %s - %d", bucket, object, size)

	// Make sure bucket path exists
	os.Mkdir(filepath.Join(siaCachePath, bucket), 0744)

	var siaObj = bucket + "/" + object
    var tmpPath = filepath.Join(siaCachePath, siaObj)

	//fmt.Println("Copying file to: "+abs(tmpPath))
	e = copyFile(data, abs(tmpPath))
	if e != nil {
		return objInfo, e
	}

	//fmt.Println("Calling POST to renter/upload source="+abs(tmpPath))
	e = post("/renter/upload/"+siaObj, "source="+abs(tmpPath))
	if e != nil {
		fmt.Println("Failure calling renter/upload")
		return objInfo, e
	}
	//fmt.Printf("Uploaded '%s' as %s.\n", abs(tmpPath), siaObj)

	return objInfo, nil
}

// CopyObject copies a blob from source container to destination container.
func (l *siaObjects) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (objInfo ObjectInfo, e error) {
	return objInfo, nil
}

// DeleteObject deletes a blob in bucket
func (l *siaObjects) DeleteObject(bucket string, object string) error {

	var siaObj = bucket + "/" + object
	
	//fmt.Println("Deleting "+ siaObj)
	err := post("/renter/delete/"+siaObj, "")
	if err != nil {
		return err
	}
	//fmt.Println("Deleted")
	return nil
}

// ListMultipartUploads lists all multipart uploads.
func (l *siaObjects) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	return lmi, nil
}

// NewMultipartUpload upload object in multiple parts
func (l *siaObjects) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	return uploadID, nil
}

// CopyObjectPart copy part of object to other bucket and object
func (l *siaObjects) CopyObjectPart(srcBucket string, srcObject string, destBucket string, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return info, nil
}

// PutObjectPart puts a part of object in bucket
func (l *siaObjects) PutObjectPart(bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (pi PartInfo, e error) {
	return pi, nil
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *siaObjects) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi ListPartsInfo, e error) {
	return lpi, nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *siaObjects) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	return nil
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (l *siaObjects) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []completePart) (oi ObjectInfo, e error) {
	return oi, nil
}

// SetBucketPolicies sets policy on bucket
func (l *siaObjects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	return nil
}

// GetBucketPolicies will get policy on bucket
func (l *siaObjects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	return policy.BucketAccessPolicy{}, nil
}

// DeleteBucketPolicies deletes all policies on bucket
func (l *siaObjects) DeleteBucketPolicies(bucket string) error {
	return nil
}
