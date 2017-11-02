/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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

package madmin

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"

	"github.com/minio/minio-go/pkg/s3signer"
	"github.com/minio/minio-go/pkg/s3utils"
)

// AdminClient implements Amazon S3 compatible methods.
type AdminClient struct {
	///  Standard options.

	// AccessKeyID required for authorized requests.
	accessKeyID string
	// SecretAccessKey required for authorized requests.
	secretAccessKey string

	// User supplied.
	appInfo struct {
		appName    string
		appVersion string
	}

	endpointURL url.URL

	// Indicate whether we are using https or not
	secure bool

	// Needs allocation.
	httpClient *http.Client

	// Advanced functionality.
	isTraceEnabled bool
	traceOutput    io.Writer

	// Random seed.
	random *rand.Rand
}

// Global constants.
const (
	libraryName    = "madmin-go"
	libraryVersion = "0.0.1"
)

// User Agent should always following the below style.
// Please open an issue to discuss any new changes here.
//
//       Minio (OS; ARCH) LIB/VER APP/VER
const (
	libraryUserAgentPrefix = "Minio (" + runtime.GOOS + "; " + runtime.GOARCH + ") "
	libraryUserAgent       = libraryUserAgentPrefix + libraryName + "/" + libraryVersion
)

// New - instantiate minio client Client, adds automatic verification of signature.
func New(endpoint string, accessKeyID, secretAccessKey string, secure bool) (*AdminClient, error) {
	clnt, err := privateNew(endpoint, accessKeyID, secretAccessKey, secure)
	if err != nil {
		return nil, err
	}
	return clnt, nil
}

func privateNew(endpoint, accessKeyID, secretAccessKey string, secure bool) (*AdminClient, error) {
	// construct endpoint.
	endpointURL, err := getEndpointURL(endpoint, secure)
	if err != nil {
		return nil, err
	}

	// instantiate new Client.
	clnt := &AdminClient{
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		// Remember whether we are using https or not
		secure: secure,
		// Save endpoint URL, user agent for future uses.
		endpointURL: *endpointURL,
		// Instantiate http client and bucket location cache.
		httpClient: &http.Client{
			Transport: http.DefaultTransport,
		},
	}

	// Return.
	return clnt, nil
}

// SetAppInfo - add application details to user agent.
func (c *AdminClient) SetAppInfo(appName string, appVersion string) {
	// if app name and version is not set, we do not a new user
	// agent.
	if appName != "" && appVersion != "" {
		c.appInfo = struct {
			appName    string
			appVersion string
		}{}
		c.appInfo.appName = appName
		c.appInfo.appVersion = appVersion
	}
}

// SetCustomTransport - set new custom transport.
func (c *AdminClient) SetCustomTransport(customHTTPTransport http.RoundTripper) {
	// Set this to override default transport
	// ``http.DefaultTransport``.
	//
	// This transport is usually needed for debugging OR to add your
	// own custom TLS certificates on the client transport, for custom
	// CA's and certs which are not part of standard certificate
	// authority follow this example :-
	//
	//   tr := &http.Transport{
	//           TLSClientConfig:    &tls.Config{RootCAs: pool},
	//           DisableCompression: true,
	//   }
	//   api.SetTransport(tr)
	//
	if c.httpClient != nil {
		c.httpClient.Transport = customHTTPTransport
	}
}

// TraceOn - enable HTTP tracing.
func (c *AdminClient) TraceOn(outputStream io.Writer) {
	// if outputStream is nil then default to os.Stdout.
	if outputStream == nil {
		outputStream = os.Stdout
	}
	// Sets a new output stream.
	c.traceOutput = outputStream

	// Enable tracing.
	c.isTraceEnabled = true
}

// TraceOff - disable HTTP tracing.
func (c *AdminClient) TraceOff() {
	// Disable tracing.
	c.isTraceEnabled = false
}

// requestMetadata - is container for all the values to make a
// request.
type requestData struct {
	customHeaders http.Header
	queryValues   url.Values

	contentBody        io.Reader
	contentLength      int64
	contentSHA256Bytes []byte
	contentMD5Bytes    []byte
}

// Filter out signature value from Authorization header.
func (c AdminClient) filterSignature(req *http.Request) {
	/// Signature V4 authorization header.

	// Save the original auth.
	origAuth := req.Header.Get("Authorization")
	// Strip out accessKeyID from:
	// Credential=<access-key-id>/<date>/<aws-region>/<aws-service>/aws4_request
	regCred := regexp.MustCompile("Credential=([A-Z0-9]+)/")
	newAuth := regCred.ReplaceAllString(origAuth, "Credential=**REDACTED**/")

	// Strip out 256-bit signature from: Signature=<256-bit signature>
	regSign := regexp.MustCompile("Signature=([[0-9a-f]+)")
	newAuth = regSign.ReplaceAllString(newAuth, "Signature=**REDACTED**")

	// Set a temporary redacted auth
	req.Header.Set("Authorization", newAuth)
	return
}

// dumpHTTP - dump HTTP request and response.
func (c AdminClient) dumpHTTP(req *http.Request, resp *http.Response) error {
	// Starts http dump.
	_, err := fmt.Fprintln(c.traceOutput, "---------START-HTTP---------")
	if err != nil {
		return err
	}

	// Filter out Signature field from Authorization header.
	c.filterSignature(req)

	// Only display request header.
	reqTrace, err := httputil.DumpRequestOut(req, false)
	if err != nil {
		return err
	}

	// Write request to trace output.
	_, err = fmt.Fprint(c.traceOutput, string(reqTrace))
	if err != nil {
		return err
	}

	// Only display response header.
	var respTrace []byte

	// For errors we make sure to dump response body as well.
	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusPartialContent &&
		resp.StatusCode != http.StatusNoContent {
		respTrace, err = httputil.DumpResponse(resp, true)
		if err != nil {
			return err
		}
	} else {
		// WORKAROUND for https://github.com/golang/go/issues/13942.
		// httputil.DumpResponse does not print response headers for
		// all successful calls which have response ContentLength set
		// to zero. Keep this workaround until the above bug is fixed.
		if resp.ContentLength == 0 {
			var buffer bytes.Buffer
			if err = resp.Header.Write(&buffer); err != nil {
				return err
			}
			respTrace = buffer.Bytes()
			respTrace = append(respTrace, []byte("\r\n")...)
		} else {
			respTrace, err = httputil.DumpResponse(resp, false)
			if err != nil {
				return err
			}
		}
	}
	// Write response to trace output.
	_, err = fmt.Fprint(c.traceOutput, strings.TrimSuffix(string(respTrace), "\r\n"))
	if err != nil {
		return err
	}

	// Ends the http dump.
	_, err = fmt.Fprintln(c.traceOutput, "---------END-HTTP---------")
	return err
}

// do - execute http request.
func (c AdminClient) do(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error
	// Do the request in a loop in case of 307 http is met since golang still doesn't
	// handle properly this situation (https://github.com/golang/go/issues/7912)
	for {
		resp, err = c.httpClient.Do(req)
		if err != nil {
			// Handle this specifically for now until future Golang
			// versions fix this issue properly.
			urlErr, ok := err.(*url.Error)
			if ok && strings.Contains(urlErr.Err.Error(), "EOF") {
				return nil, &url.Error{
					Op:  urlErr.Op,
					URL: urlErr.URL,
					Err: fmt.Errorf("Connection closed by foreign host %s", urlErr.URL),
				}
			}
			return nil, err
		}
		// Redo the request with the new redirect url if http 307 is returned, quit the loop otherwise
		if resp != nil && resp.StatusCode == http.StatusTemporaryRedirect {
			newURL, uErr := url.Parse(resp.Header.Get("Location"))
			if uErr != nil {
				break
			}
			req.URL = newURL
		} else {
			break
		}
	}

	// Response cannot be non-nil, report if its the case.
	if resp == nil {
		msg := "Response is empty. " // + reportIssue
		return nil, ErrInvalidArgument(msg)
	}

	// If trace is enabled, dump http request and response.
	if c.isTraceEnabled {
		err = c.dumpHTTP(req, resp)
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// List of success status.
var successStatus = []int{
	http.StatusOK,
	http.StatusNoContent,
	http.StatusPartialContent,
}

// executeMethod - instantiates a given method, and retries the
// request upon any error up to maxRetries attempts in a binomially
// delayed manner using a standard back off algorithm.
func (c AdminClient) executeMethod(method string, reqData requestData) (res *http.Response, err error) {

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{}, 1)

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// Instantiate a new request.
	var req *http.Request
	req, err = c.newRequest(method, reqData)
	if err != nil {
		return nil, err
	}

	// Initiate the request.
	res, err = c.do(req)
	if err != nil {
		return nil, err
	}

	// For any known successful http status, return quickly.
	for _, httpStatus := range successStatus {
		if httpStatus == res.StatusCode {
			return res, nil
		}
	}

	// Read the body to be saved later.
	errBodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	// Save the body.
	errBodySeeker := bytes.NewReader(errBodyBytes)
	res.Body = ioutil.NopCloser(errBodySeeker)

	// Save the body back again.
	errBodySeeker.Seek(0, 0) // Seek back to starting point.
	res.Body = ioutil.NopCloser(errBodySeeker)

	return res, err
}

// set User agent.
func (c AdminClient) setUserAgent(req *http.Request) {
	req.Header.Set("User-Agent", libraryUserAgent)
	if c.appInfo.appName != "" && c.appInfo.appVersion != "" {
		req.Header.Set("User-Agent", libraryUserAgent+" "+c.appInfo.appName+"/"+c.appInfo.appVersion)
	}
}

// newRequest - instantiate a new HTTP request for a given method.
func (c AdminClient) newRequest(method string, reqData requestData) (req *http.Request, err error) {
	// If no method is supplied default to 'POST'.
	if method == "" {
		method = "POST"
	}

	// Default all requests to "us-east-1"
	location := "us-east-1"

	// Construct a new target URL.
	targetURL, err := c.makeTargetURL(reqData.queryValues)
	if err != nil {
		return nil, err
	}

	// Initialize a new HTTP request for the method.
	req, err = http.NewRequest(method, targetURL.String(), nil)
	if err != nil {
		return nil, err
	}

	// Set content body if available.
	if reqData.contentBody != nil {
		req.Body = ioutil.NopCloser(reqData.contentBody)
	}

	// Set 'User-Agent' header for the request.
	c.setUserAgent(req)

	// Set all headers.
	for k, v := range reqData.customHeaders {
		req.Header.Set(k, v[0])
	}

	// set incoming content-length.
	if reqData.contentLength > 0 {
		req.ContentLength = reqData.contentLength
	}

	shaHeader := unsignedPayload
	if !c.secure {
		if reqData.contentSHA256Bytes == nil {
			shaHeader = hex.EncodeToString(sum256([]byte{}))
		} else {
			shaHeader = hex.EncodeToString(reqData.contentSHA256Bytes)
		}
	}
	req.Header.Set("X-Amz-Content-Sha256", shaHeader)

	// set md5Sum for content protection.
	if reqData.contentMD5Bytes != nil {
		req.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(reqData.contentMD5Bytes))
	}

	// Add signature version '4' authorization header.
	req = s3signer.SignV4(*req, c.accessKeyID, c.secretAccessKey, "", location)

	// Return request.
	return req, nil
}

// makeTargetURL make a new target url.
func (c AdminClient) makeTargetURL(queryValues url.Values) (*url.URL, error) {

	host := c.endpointURL.Host
	scheme := c.endpointURL.Scheme

	urlStr := scheme + "://" + host + "/"

	// If there are any query values, add them to the end.
	if len(queryValues) > 0 {
		urlStr = urlStr + "?" + s3utils.QueryEncode(queryValues)
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	return u, nil
}
