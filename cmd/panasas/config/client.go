package config

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

const panasasHTTPS3PrefixHeader string = "x-panasas-s3-prefix"
const panasasHTTPS3MetadataHeader string = "x-panasas-s3-metadata"
const panasasHTTPS3ConfigRevisionHeader string = "x-panasas-s3-config-revision"

// ErrNotFound informs that the requested object has not been found by the
// config agent.
var ErrNotFound = fmt.Errorf("Not found")

// ErrNoRevisionYet informs that no successful modifying operation has been
// performed yet and consequently there is not cached config revision.
var ErrNoRevisionYet = fmt.Errorf("Config revision not retrieved yet")

// LockExpirationMilliseconds – time in milliseconds after which the Panasas config agent
// automatically releases a lock
const LockExpirationMilliseconds int = 3000

// ErrUnexpectedHTTPStatus informs about HTTP status value outside the expected
// range.
type ErrUnexpectedHTTPStatus uint

// ErrUnexpectedContentType informs about unexpected HTTP response content type
type ErrUnexpectedContentType string

func (e ErrUnexpectedHTTPStatus) Error() string {
	return fmt.Sprintf("Unexpected HTTP status: %v", uint(e))
}

func (e ErrUnexpectedContentType) Error() string {
	return fmt.Sprintf("Unexpected HTTP content type: %q", string(e))
}

// PanasasConfigClient represents a Panasas config agent client
type PanasasConfigClient struct {
	agentHost  string
	agentPort  uint
	httpClient *http.Client
	protocol   string

	configRevision *string
}

// NewPanasasConfigClient returns a configured PanasasConfigClient
func NewPanasasConfigClient(host string, port uint) *PanasasConfigClient {
	client := PanasasConfigClient{
		agentHost:  host,
		agentPort:  port,
		httpClient: &http.Client{},
		protocol:   "http",
	}
	return &client
}

func (c *PanasasConfigClient) getPanasasConfigStoreURL(base string, elem ...string) (*url.URL, error) {
	var path string = base
	var e error

	for _, eachElem := range elem {
		path, e = url.JoinPath(base, url.PathEscape(eachElem))
		if e != nil {
			return nil, e
		}
	}

	return url.Parse(
		fmt.Sprintf("%v://%v:%v%v", c.protocol, c.agentHost, c.agentPort, path),
	)
}

func (c *PanasasConfigClient) makePanasasConfigAgentRequest(urlBase string, urlElem ...string) (*http.Request, error) {
	u, err := c.getPanasasConfigStoreURL(urlBase, urlElem...)
	if err != nil {
		fmt.Printf("Failed formatting URL for %v\n", urlBase)
		log.Fatal(err)
		return nil, err
	}

	req := http.Request{
		Method:        http.MethodGet,
		URL:           u,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Host:          u.Host,
		ContentLength: 0,
	}
	return &req, nil
}

func (c *PanasasConfigClient) extractConfigRevision(resp *http.Response) {
	revisionHeaders := resp.Header.Values(panasasHTTPS3ConfigRevisionHeader)
	if len(revisionHeaders) > 0 {
		var revision string = revisionHeaders[0]
		c.configRevision = &revision
	}
}

// closeResponseBody close non nil response with any response Body.
// convenient wrapper to drain any remaining data on response body.
//
// Subsequently this allows golang http RoundTripper
// to re-use the same connection for future requests.
// (copied from minio-go)
func closeResponseBody(resp *http.Response) {
	// Callers should close resp.Body when done reading from it.
	// If resp.Body is not closed, the Client's underlying RoundTripper
	// (typically Transport) may not be able to re-use a persistent TCP
	// connection to the server for a subsequent "keep-alive" request.
	if resp != nil && resp.Body != nil {
		// Drain any remaining Body and then close the connection.
		// Without this closing connection would disallow re-using
		// the same connection for future uses.
		//  - http://stackoverflow.com/a/17961593/4465767
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
}

// PanasasObjectList represents a list of objects as returned by configuration agent
type PanasasObjectList struct {
	Objects []string
}

// GetObjectsList returns a list of objects
func (c *PanasasConfigClient) GetObjectsList(prefix string) ([]string, error) {
	req, err := c.makePanasasConfigAgentRequest("/objects")
	if err != nil {
		fmt.Printf("Failed preparing HTTP request object for /objects with prefix %q\n", prefix)
		log.Fatal(err)
		return []string{}, err
	}

	req.Header.Set(panasasHTTPS3PrefixHeader, prefix)

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		log.Fatal(err)
		return []string{}, err
	}
	if contentType := resp.Header.Get("content-type"); contentType != "application/json" {
		return []string{}, ErrUnexpectedContentType(contentType)
	}

	dec := json.NewDecoder(resp.Body)
	var result PanasasObjectList
	err = dec.Decode(&result)

	if err != nil {
		log.Fatal(err)
		return []string{}, err
	}

	return result.Objects, nil
}

// GetObject returns an object:
// - dataReader is the reader returning the data of the object,
// - metadata is the metadata of the object.
// The caller is responsible for calling Close() on the dataReader.
// If err is not nil, dataReader and metadata are assumed invalid and should
// not be used.
func (c *PanasasConfigClient) GetObject(objectName string) (dataReader io.ReadCloser, metadata string, err error) {
	base := "/objects"

	req, err := c.makePanasasConfigAgentRequest(base, objectName)
	if err != nil {
		fmt.Printf("Failed preparing HTTP request object for %v with name %q\n", base, objectName)
		log.Fatal(err)
		return nil, "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("HTTP request failed with error %w", err)
	}

	defer func() {
		if err != nil {
			closeResponseBody(resp)
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		return nil, "", ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	if contentType := resp.Header.Get("content-type"); contentType != "application/octet-stream" {
		err = ErrUnexpectedContentType(contentType)
		return
	}

	metadata = resp.Header.Get(panasasHTTPS3MetadataHeader)

	return resp.Body, metadata, nil
}

// DeleteObject requests an object to be deleted from the config agent
func (c *PanasasConfigClient) DeleteObject(objectName string, lockID ...string) error {
	base := "/objects"

	req, err := c.makePanasasConfigAgentRequest(base, objectName)
	if err != nil {
		fmt.Printf("Failed preparing HTTP request object for %v with name %q\n", base, objectName)
		log.Fatal(err)
		return err
	}

	req.Method = http.MethodDelete

	if len(lockID) != 0 && lockID[0] != "" {
		q := req.URL.Query()
		q.Add("lock_id", lockID[0])

		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return fmt.Errorf("HTTP request failed with error %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ErrUnexpectedHTTPStatus(resp.StatusCode)
	}
	c.extractConfigRevision(resp)

	return nil
}

// PutObject stores an object of a given name in the config agent
func (c *PanasasConfigClient) PutObject(objectName string, data io.Reader, metadata string, lockID ...string) (created bool, e error) {
	// TODO: set the modification time:
	// internal/rest/client.go:206:
	// req.Header.Set("X-Minio-Time", time.Now().UTC().Format(time.RFC3339))

	base := "/objects"

	req, err := c.makePanasasConfigAgentRequest(base, objectName)
	if err != nil {
		fmt.Printf("Failed preparing HTTP request object for %v with name %q\n", base, objectName)
		log.Fatal(err)
		return false, err
	}

	body := io.NopCloser(data)

	req.Method = http.MethodPut
	req.Body = body
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(panasasHTTPS3MetadataHeader, metadata)

	if len(lockID) != 0 && lockID[0] != "" {
		q := req.URL.Query()
		q.Add("lock_id", lockID[0])

		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.httpClient.Do(req)
	closeResponseBody(resp)
	if err != nil {
		return false, fmt.Errorf("HTTP request failed with error %w", err)
	}
	if resp.StatusCode == http.StatusOK {
		c.extractConfigRevision(resp)
		return false, nil
	} else if resp.StatusCode == http.StatusCreated {
		c.extractConfigRevision(resp)
		return true, nil
	}

	return false, ErrUnexpectedHTTPStatus(resp.StatusCode)
}

// GetObjectInfo fetches object metadata from the config agent
func (c *PanasasConfigClient) GetObjectInfo(objectName string) (metadata string, err error) {
	base := "/objects"

	req, err := c.makePanasasConfigAgentRequest(base, objectName)
	if err != nil {
		fmt.Printf("Failed preparing HTTP request object for %v with name %q\n", base, objectName)
		log.Fatal(err)
		return "", err
	}

	req.Method = http.MethodHead
	resp, err := c.httpClient.Do(req)
	closeResponseBody(resp)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed with error %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	metadata = resp.Header.Get(panasasHTTPS3MetadataHeader)

	return metadata, nil
}

// GetRecentConfigRevision returns the config revision reported by the most
// recent config modifying operation (PutObject/DeleteObject)
func (c *PanasasConfigClient) GetRecentConfigRevision() (revision string, err error) {
	if c.configRevision == nil {
		return "", ErrNoRevisionYet
	}
	return *c.configRevision, nil
}

// GetConfigRevision queries the config agent for the current config revision
func (c *PanasasConfigClient) GetConfigRevision() (revision string, err error) {
	req, err := c.makePanasasConfigAgentRequest("/revision")
	if err != nil {
		fmt.Printf("Failed preparing HTTP request object for /revision\n")
		log.Fatal(err)
		return "", err
	}

	resp, err := c.httpClient.Do(req)
	closeResponseBody(resp)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed with error %w", err)
	}
	if resp.StatusCode == http.StatusOK {
		return resp.Header.Get(panasasHTTPS3ConfigRevisionHeader), nil
	}
	return "", ErrUnexpectedHTTPStatus(resp.StatusCode)
}

// GetObjectLock tries to get a write or read lock on the specified object
// Set "read" to true to obtain a non-exclusive read lock.
// Returns the ID of the obtained lock. This ID can be then used in the calls
// to ReleaseObjectLock(), PutObject(), DeleteObject().
func (c *PanasasConfigClient) GetObjectLock(objectName string, read bool) (lockID string, err error) {
	base := "/lock"

	req, err := c.makePanasasConfigAgentRequest(base, objectName)
	if err != nil {
		fmt.Printf("Failed preparing HTTP request object for %v with name %q\n", base, objectName)
		log.Fatal(err)
		return "", err
	}

	req.Method = http.MethodPost

	if read != false {
		q := req.URL.Query()
		q.Add("type", "read")

		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed with error %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	expectedContentType := "text/plain"
	if contentType := resp.Header.Get("content-type"); contentType != expectedContentType{
		err = ErrUnexpectedContentType(contentType)
		return
	}

	lockIDBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	lockID = string(lockIDBytes)
	return lockID, nil
}

// ReleaseObjectLock releases a previously obtained object lock
// Will return ErrNotFound if the specified lock has not been found or the lock
// has expired since it was obtained.
func (c *PanasasConfigClient) ReleaseObjectLock(lockID string) (err error) {
	base := "/locks"
	req, err := c.makePanasasConfigAgentRequest(base, lockID)
	if err != nil {
		fmt.Printf("Failed preparing HTTP request object for %v with name %q\n", base, lockID)
		log.Fatal(err)
		return err
	}

	req.Method = http.MethodDelete

	resp, err := c.httpClient.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return fmt.Errorf("HTTP request failed with error %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ErrUnexpectedHTTPStatus(resp.StatusCode)
	}

	return nil
}
