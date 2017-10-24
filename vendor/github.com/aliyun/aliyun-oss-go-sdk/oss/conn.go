package oss

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Conn oss conn
type Conn struct {
	config *Config
	url    *urlMaker
	client *http.Client
}

var signKeyList = []string{"acl", "uploads", "location", "cors", "logging", "website", "referer", "lifecycle", "delete", "append", "tagging", "objectMeta", "uploadId", "partNumber", "security-token", "position", "img", "style", "styleName", "replication", "replicationProgress", "replicationLocation", "cname", "bucketInfo", "comp", "qos", "live", "status", "vod", "startTime", "endTime", "symlink", "x-oss-process", "response-content-type", "response-content-language", "response-expires", "response-cache-control", "response-content-disposition", "response-content-encoding", "udf", "udfName", "udfImage", "udfId", "udfImageDesc", "udfApplication", "comp", "udfApplicationLog", "restore"}

// init 初始化Conn
func (conn *Conn) init(config *Config, urlMaker *urlMaker) error {
	httpTimeOut := conn.config.HTTPTimeout

	// new Transport
	transport := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			conn, err := net.DialTimeout(netw, addr, httpTimeOut.ConnectTimeout)
			if err != nil {
				return nil, err
			}
			return newTimeoutConn(conn, httpTimeOut.ReadWriteTimeout, httpTimeOut.LongTimeout), nil
		},
		ResponseHeaderTimeout: httpTimeOut.HeaderTimeout,
	}

	// Proxy
	if conn.config.IsUseProxy {
		proxyURL, err := url.Parse(config.ProxyHost)
		if err != nil {
			return err
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	conn.config = config
	conn.url = urlMaker
	conn.client = &http.Client{Transport: transport}

	return nil
}

// Do 处理请求，返回响应结果。
func (conn Conn) Do(method, bucketName, objectName string, params map[string]interface{}, headers map[string]string,
	data io.Reader, initCRC uint64, listener ProgressListener) (*Response, error) {
	urlParams := conn.getURLParams(params)
	subResource := conn.getSubResource(params)
	uri := conn.url.getURL(bucketName, objectName, urlParams)
	resource := conn.url.getResource(bucketName, objectName, subResource)
	return conn.doRequest(method, uri, resource, headers, data, initCRC, listener)
}

// DoURL 根据已签名的URL处理请求，返回响应结果。
func (conn Conn) DoURL(method HTTPMethod, signedURL string, headers map[string]string,
	data io.Reader, initCRC uint64, listener ProgressListener) (*Response, error) {
	// get uri form signedURL
	uri, err := url.ParseRequestURI(signedURL)
	if err != nil {
		return nil, err
	}

	m := strings.ToUpper(string(method))
	req := &http.Request{
		Method:     m,
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}

	tracker := &readerTracker{completedBytes: 0}
	fd, crc := conn.handleBody(req, data, initCRC, listener, tracker)
	if fd != nil {
		defer func() {
			fd.Close()
			os.Remove(fd.Name())
		}()
	}

	if conn.config.IsAuthProxy {
		auth := conn.config.ProxyUser + ":" + conn.config.ProxyPassword
		basic := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Proxy-Authorization", basic)
	}

	req.Header.Set(HTTPHeaderHost, conn.config.Endpoint)
	req.Header.Set(HTTPHeaderUserAgent, conn.config.UserAgent)

	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	// transfer started
	event := newProgressEvent(TransferStartedEvent, 0, req.ContentLength)
	publishProgress(listener, event)

	resp, err := conn.client.Do(req)
	if err != nil {
		// transfer failed
		event = newProgressEvent(TransferFailedEvent, tracker.completedBytes, req.ContentLength)
		publishProgress(listener, event)
		return nil, err
	}

	// transfer completed
	event = newProgressEvent(TransferCompletedEvent, tracker.completedBytes, req.ContentLength)
	publishProgress(listener, event)

	return conn.handleResponse(resp, crc)
}

func (conn Conn) getURLParams(params map[string]interface{}) string {
	// sort
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// serialize
	var buf bytes.Buffer
	for _, k := range keys {
		if buf.Len() > 0 {
			buf.WriteByte('&')
		}
		buf.WriteString(url.QueryEscape(k))
		if params[k] != nil {
			buf.WriteString("=" + url.QueryEscape(params[k].(string)))
		}
	}

	return buf.String()
}

func (conn Conn) getSubResource(params map[string]interface{}) string {
	// sort
	keys := make([]string, 0, len(params))
	for k := range params {
		if conn.isParamSign(k) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	// serialize
	var buf bytes.Buffer
	for _, k := range keys {
		if buf.Len() > 0 {
			buf.WriteByte('&')
		}
		buf.WriteString(k)
		if params[k] != nil {
			buf.WriteString("=" + params[k].(string))
		}
	}

	return buf.String()
}

func (conn Conn) isParamSign(paramKey string) bool {
	for _, k := range signKeyList {
		if paramKey == k {
			return true
		}
	}
	return false
}

func (conn Conn) doRequest(method string, uri *url.URL, canonicalizedResource string, headers map[string]string,
	data io.Reader, initCRC uint64, listener ProgressListener) (*Response, error) {
	method = strings.ToUpper(method)
	req := &http.Request{
		Method:     method,
		URL:        uri,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       uri.Host,
	}

	tracker := &readerTracker{completedBytes: 0}
	fd, crc := conn.handleBody(req, data, initCRC, listener, tracker)
	if fd != nil {
		defer func() {
			fd.Close()
			os.Remove(fd.Name())
		}()
	}

	if conn.config.IsAuthProxy {
		auth := conn.config.ProxyUser + ":" + conn.config.ProxyPassword
		basic := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Proxy-Authorization", basic)
	}

	date := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set(HTTPHeaderDate, date)
	req.Header.Set(HTTPHeaderHost, conn.config.Endpoint)
	req.Header.Set(HTTPHeaderUserAgent, conn.config.UserAgent)
	if conn.config.SecurityToken != "" {
		req.Header.Set(HTTPHeaderOssSecurityToken, conn.config.SecurityToken)
	}

	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	conn.signHeader(req, canonicalizedResource)

	// transfer started
	event := newProgressEvent(TransferStartedEvent, 0, req.ContentLength)
	publishProgress(listener, event)

	resp, err := conn.client.Do(req)
	if err != nil {
		// transfer failed
		event = newProgressEvent(TransferFailedEvent, tracker.completedBytes, req.ContentLength)
		publishProgress(listener, event)
		return nil, err
	}

	// transfer completed
	event = newProgressEvent(TransferCompletedEvent, tracker.completedBytes, req.ContentLength)
	publishProgress(listener, event)

	return conn.handleResponse(resp, crc)
}

func (conn Conn) signURL(method HTTPMethod, bucketName, objectName string, expiration int64, params map[string]interface{}, headers map[string]string) string {
	subResource := conn.getSubResource(params)
	canonicalizedResource := conn.url.getResource(bucketName, objectName, subResource)

	m := strings.ToUpper(string(method))
	req := &http.Request{
		Method: m,
		Header: make(http.Header),
	}

	if conn.config.IsAuthProxy {
		auth := conn.config.ProxyUser + ":" + conn.config.ProxyPassword
		basic := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Proxy-Authorization", basic)
	}

	req.Header.Set(HTTPHeaderDate, strconv.FormatInt(expiration, 10))
	req.Header.Set(HTTPHeaderHost, conn.config.Endpoint)
	req.Header.Set(HTTPHeaderUserAgent, conn.config.UserAgent)

	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	signedStr := conn.getSignedStr(req, canonicalizedResource)

	params[HTTPParamExpires] = strconv.FormatInt(expiration, 10)
	params[HTTPParamAccessKeyID] = conn.config.AccessKeyID
	params[HTTPParamSignature] = signedStr
	if conn.config.SecurityToken != "" {
		params[HTTPParamSecurityToken] = conn.config.SecurityToken
	}

	urlParams := conn.getURLParams(params)
	return conn.url.getSignURL(bucketName, objectName, urlParams)
}

// handle request body
func (conn Conn) handleBody(req *http.Request, body io.Reader, initCRC uint64,
	listener ProgressListener, tracker *readerTracker) (*os.File, hash.Hash64) {
	var file *os.File
	var crc hash.Hash64
	reader := body

	// length
	switch v := body.(type) {
	case *bytes.Buffer:
		req.ContentLength = int64(v.Len())
	case *bytes.Reader:
		req.ContentLength = int64(v.Len())
	case *strings.Reader:
		req.ContentLength = int64(v.Len())
	case *os.File:
		req.ContentLength = tryGetFileSize(v)
	case *io.LimitedReader:
		req.ContentLength = int64(v.N)
	}
	req.Header.Set(HTTPHeaderContentLength, strconv.FormatInt(req.ContentLength, 10))

	// md5
	if body != nil && conn.config.IsEnableMD5 && req.Header.Get(HTTPHeaderContentMD5) == "" {
		md5 := ""
		reader, md5, file, _ = calcMD5(body, req.ContentLength, conn.config.MD5Threshold)
		req.Header.Set(HTTPHeaderContentMD5, md5)
	}

	// crc
	if reader != nil && conn.config.IsEnableCRC {
		crc = NewCRC(crcTable(), initCRC)
		reader = TeeReader(reader, crc, req.ContentLength, listener, tracker)
	}

	// http body
	rc, ok := reader.(io.ReadCloser)
	if !ok && reader != nil {
		rc = ioutil.NopCloser(reader)
	}
	req.Body = rc

	return file, crc
}

func tryGetFileSize(f *os.File) int64 {
	fInfo, _ := f.Stat()
	return fInfo.Size()
}

// handle response
func (conn Conn) handleResponse(resp *http.Response, crc hash.Hash64) (*Response, error) {
	var cliCRC uint64
	var srvCRC uint64

	statusCode := resp.StatusCode
	if statusCode >= 400 && statusCode <= 505 {
		// 4xx and 5xx indicate that the operation has error occurred
		var respBody []byte
		respBody, err := readResponseBody(resp)
		if err != nil {
			return nil, err
		}

		if len(respBody) == 0 {
			// no error in response body
			err = fmt.Errorf("oss: service returned without a response body (%s)", resp.Status)
		} else {
			// response contains storage service error object, unmarshal
			srvErr, errIn := serviceErrFromXML(respBody, resp.StatusCode,
				resp.Header.Get(HTTPHeaderOssRequestID))
			if err != nil { // error unmarshaling the error response
				err = errIn
			}
			err = srvErr
		}

		return &Response{
			StatusCode: resp.StatusCode,
			Headers:    resp.Header,
			Body:       ioutil.NopCloser(bytes.NewReader(respBody)), // restore the body
		}, err
	} else if statusCode >= 300 && statusCode <= 307 {
		// oss use 3xx, but response has no body
		err := fmt.Errorf("oss: service returned %d,%s", resp.StatusCode, resp.Status)
		return &Response{
			StatusCode: resp.StatusCode,
			Headers:    resp.Header,
			Body:       resp.Body,
		}, err
	}

	if conn.config.IsEnableCRC && crc != nil {
		cliCRC = crc.Sum64()
	}
	srvCRC, _ = strconv.ParseUint(resp.Header.Get(HTTPHeaderOssCRC64), 10, 64)

	// 2xx, successful
	return &Response{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
		Body:       resp.Body,
		ClientCRC:  cliCRC,
		ServerCRC:  srvCRC,
	}, nil
}

func calcMD5(body io.Reader, contentLen, md5Threshold int64) (reader io.Reader, b64 string, tempFile *os.File, err error) {
	if contentLen == 0 || contentLen > md5Threshold {
		// huge body, use temporary file
		tempFile, err = ioutil.TempFile(os.TempDir(), TempFilePrefix)
		if tempFile != nil {
			io.Copy(tempFile, body)
			tempFile.Seek(0, os.SEEK_SET)
			md5 := md5.New()
			io.Copy(md5, tempFile)
			sum := md5.Sum(nil)
			b64 = base64.StdEncoding.EncodeToString(sum[:])
			tempFile.Seek(0, os.SEEK_SET)
			reader = tempFile
		}
	} else {
		// small body, use memory
		buf, _ := ioutil.ReadAll(body)
		sum := md5.Sum(buf)
		b64 = base64.StdEncoding.EncodeToString(sum[:])
		reader = bytes.NewReader(buf)
	}
	return
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	out, err := ioutil.ReadAll(resp.Body)
	if err == io.EOF {
		err = nil
	}
	return out, err
}

func serviceErrFromXML(body []byte, statusCode int, requestID string) (ServiceError, error) {
	var storageErr ServiceError
	if err := xml.Unmarshal(body, &storageErr); err != nil {
		return storageErr, err
	}
	storageErr.StatusCode = statusCode
	storageErr.RequestID = requestID
	storageErr.RawMessage = string(body)
	return storageErr, nil
}

func xmlUnmarshal(body io.Reader, v interface{}) error {
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}
	return xml.Unmarshal(data, v)
}

// Handle http timeout
type timeoutConn struct {
	conn        net.Conn
	timeout     time.Duration
	longTimeout time.Duration
}

func newTimeoutConn(conn net.Conn, timeout time.Duration, longTimeout time.Duration) *timeoutConn {
	conn.SetReadDeadline(time.Now().Add(longTimeout))
	return &timeoutConn{
		conn:        conn,
		timeout:     timeout,
		longTimeout: longTimeout,
	}
}

func (c *timeoutConn) Read(b []byte) (n int, err error) {
	c.SetReadDeadline(time.Now().Add(c.timeout))
	n, err = c.conn.Read(b)
	c.SetReadDeadline(time.Now().Add(c.longTimeout))
	return n, err
}

func (c *timeoutConn) Write(b []byte) (n int, err error) {
	c.SetWriteDeadline(time.Now().Add(c.timeout))
	n, err = c.conn.Write(b)
	c.SetReadDeadline(time.Now().Add(c.longTimeout))
	return n, err
}

func (c *timeoutConn) Close() error {
	return c.conn.Close()
}

func (c *timeoutConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *timeoutConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *timeoutConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *timeoutConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *timeoutConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// UrlMaker - build url and resource
const (
	urlTypeCname  = 1
	urlTypeIP     = 2
	urlTypeAliyun = 3
)

type urlMaker struct {
	Scheme  string // http or https
	NetLoc  string // host or ip
	Type    int    // 1 CNAME 2 IP 3 ALIYUN
	IsProxy bool   // proxy
}

// Parse endpoint
func (um *urlMaker) Init(endpoint string, isCname bool, isProxy bool) {
	if strings.HasPrefix(endpoint, "http://") {
		um.Scheme = "http"
		um.NetLoc = endpoint[len("http://"):]
	} else if strings.HasPrefix(endpoint, "https://") {
		um.Scheme = "https"
		um.NetLoc = endpoint[len("https://"):]
	} else {
		um.Scheme = "http"
		um.NetLoc = endpoint
	}

	host, _, err := net.SplitHostPort(um.NetLoc)
	if err != nil {
		host = um.NetLoc
	}
	ip := net.ParseIP(host)
	if ip != nil {
		um.Type = urlTypeIP
	} else if isCname {
		um.Type = urlTypeCname
	} else {
		um.Type = urlTypeAliyun
	}
	um.IsProxy = isProxy
}

// Build URL
func (um urlMaker) getURL(bucket, object, params string) *url.URL {
	host, path := um.buildURL(bucket, object)
	addr := ""
	if params == "" {
		addr = fmt.Sprintf("%s://%s%s", um.Scheme, host, path)
	} else {
		addr = fmt.Sprintf("%s://%s%s?%s", um.Scheme, host, path, params)
	}
	uri, _ := url.ParseRequestURI(addr)
	return uri
}

// Build Sign URL
func (um urlMaker) getSignURL(bucket, object, params string) string {
	host, path := um.buildURL(bucket, object)
	return fmt.Sprintf("%s://%s%s?%s", um.Scheme, host, path, params)
}

// Build URL
func (um urlMaker) buildURL(bucket, object string) (string, string) {
	var host = ""
	var path = ""

	object = url.QueryEscape(object)
	object = strings.Replace(object, "+", "%20", -1)

	if um.Type == urlTypeCname {
		host = um.NetLoc
		path = "/" + object
	} else if um.Type == urlTypeIP {
		if bucket == "" {
			host = um.NetLoc
			path = "/"
		} else {
			host = um.NetLoc
			path = fmt.Sprintf("/%s/%s", bucket, object)
		}
	} else {
		if bucket == "" {
			host = um.NetLoc
			path = "/"
		} else {
			host = bucket + "." + um.NetLoc
			path = "/" + object
		}
	}

	return host, path
}

// Canonicalized Resource
func (um urlMaker) getResource(bucketName, objectName, subResource string) string {
	if subResource != "" {
		subResource = "?" + subResource
	}
	if bucketName == "" {
		return fmt.Sprintf("/%s%s", bucketName, subResource)
	}
	return fmt.Sprintf("/%s/%s%s", bucketName, objectName, subResource)
}
