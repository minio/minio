// Copyright 2016, Google
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package base provides a very low-level interface on top of the B2 v1 API.
// It is not intended to be used directly.
//
// It currently lacks support for the following APIs:
//
// b2_download_file_by_id
// b2_list_unfinished_large_files
package base

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/blazer/internal/b2types"
	"github.com/minio/blazer/internal/blog"
)

const (
	APIBase          = "https://api.backblazeb2.com"
	DefaultUserAgent = "blazer/0.1.1"
)

type b2err struct {
	msg        string
	method     string
	retry      int
	statusCode int
	code       string
}

func (e b2err) Error() string {
	if e.method == "" {
		return fmt.Sprintf("b2 error: %s", e.msg)
	}
	return fmt.Sprintf("%s: %d: %s: %s", e.method, e.statusCode, e.code, e.msg)
}

// Action checks an error and returns a recommended course of action.
func Action(err error) ErrAction {
	e, ok := err.(b2err)
	if !ok {
		return Punt
	}
	if e.retry > 0 {
		return Retry
	}
	if e.statusCode >= http.StatusInternalServerError && e.statusCode < 600 {
		if e.method == "b2_upload_file" || e.method == "b2_upload_part" {
			return AttemptNewUpload
		}
	}
	switch e.statusCode {
	case http.StatusUnauthorized:
		if e.method == "b2_authorize_account" {
			return Punt
		}
		if e.method == "b2_upload_file" || e.method == "b2_upload_part" {
			return AttemptNewUpload
		}
		return ReAuthenticate
	case http.StatusBadRequest:
		// See restic/restic#1207
		if e.method == "b2_upload_file" && strings.HasPrefix(e.msg, "more than one upload using auth token") {
			return AttemptNewUpload
		}
		return Punt
	case http.StatusRequestTimeout:
		return AttemptNewUpload
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable:
		return Retry
	}
	return Punt
}

// ErrAction is an action that a caller can take when any function returns an
// error.
type ErrAction int

// Code returns the error code and message.
func Code(err error) (int, string, string) {
	e, ok := err.(b2err)
	if !ok {
		return 0, "", ""
	}
	return e.statusCode, e.code, e.msg
}

const (
	// ReAuthenticate indicates that the B2 account authentication tokens have
	// expired, and should be refreshed with a new call to AuthorizeAccount.
	ReAuthenticate ErrAction = iota

	// AttemptNewUpload indicates that an upload's authentication token (or URL
	// endpoint) has expired, and that users should request new ones with a call
	// to GetUploadURL or GetUploadPartURL.
	AttemptNewUpload

	// Retry indicates that the caller should wait an appropriate amount of time,
	// and then reattempt the RPC.
	Retry

	// Punt means that there is no useful action to be taken on this error, and
	// that it should be displayed to the user.
	Punt
)

func mkErr(resp *http.Response) error {
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	logResponse(resp, data)
	msg := &b2types.ErrorMessage{}
	if err := json.Unmarshal(data, msg); err != nil {
		return err
	}
	var retryAfter int
	retry := resp.Header.Get("Retry-After")
	if retry != "" {
		r, err := strconv.ParseInt(retry, 10, 64)
		if err != nil {
			return err
		}
		retryAfter = int(r)
	}
	return b2err{
		msg:        msg.Msg,
		retry:      retryAfter,
		statusCode: resp.StatusCode,
		code:       msg.Code,
		method:     resp.Request.Header.Get("X-Blazer-Method"),
	}
}

// Backoff returns an appropriate amount of time to wait, given an error, if
// any was returned by the server.  If the return value is 0, but Action
// indicates Retry, the user should implement their own exponential backoff,
// beginning with one second.
func Backoff(err error) time.Duration {
	e, ok := err.(b2err)
	if !ok {
		return 0
	}
	return time.Duration(e.retry) * time.Second
}

func logRequest(req *http.Request, args []byte) {
	if !blog.V(2) {
		return
	}
	var headers []string
	for k, v := range req.Header {
		if k == "Authorization" || k == "X-Blazer-Method" {
			continue
		}
		headers = append(headers, fmt.Sprintf("%s: %s", k, strings.Join(v, ",")))
	}
	hstr := strings.Join(headers, ";")
	method := req.Header.Get("X-Blazer-Method")
	if args != nil {
		blog.V(2).Infof(">> %s uri: %v headers: {%s} args: (%s)", method, req.URL, hstr, string(args))
		return
	}
	blog.V(2).Infof(">> %s uri: %v {%s} (no args)", method, req.URL, hstr)
}

var authRegexp = regexp.MustCompile(`"authorizationToken": ".[^"]*"`)

func logResponse(resp *http.Response, reply []byte) {
	if !blog.V(2) {
		return
	}
	var headers []string
	for k, v := range resp.Header {
		headers = append(headers, fmt.Sprintf("%s: %s", k, strings.Join(v, ",")))
	}
	hstr := strings.Join(headers, "; ")
	method := resp.Request.Header.Get("X-Blazer-Method")
	id := resp.Request.Header.Get("X-Blazer-Request-ID")
	if reply != nil {
		safe := string(authRegexp.ReplaceAll(reply, []byte(`"authorizationToken": "[redacted]"`)))
		blog.V(2).Infof("<< %s (%s) %s {%s} (%s)", method, id, resp.Status, hstr, safe)
		return
	}
	blog.V(2).Infof("<< %s (%s) %s {%s} (no reply)", method, id, resp.Status, hstr)
}

func millitime(t int64) time.Time {
	return time.Unix(t/1000, t%1000*1e6)
}

type b2Options struct {
	transport       http.RoundTripper
	failSomeUploads bool
	expireTokens    bool
	capExceeded     bool
	apiBase         string
	userAgent       string
}

func (o *b2Options) getAPIBase() string {
	if o.apiBase != "" {
		return o.apiBase
	}
	return APIBase
}

func (o *b2Options) getUserAgent() string {
	if o.userAgent != "" {
		return fmt.Sprintf("%s %s", o.userAgent, DefaultUserAgent)
	}
	return DefaultUserAgent
}

func (o *b2Options) getTransport() http.RoundTripper {
	if o.transport == nil {
		return http.DefaultTransport
	}
	return o.transport
}

// B2 holds account information for Backblaze.
type B2 struct {
	accountID   string
	authToken   string
	apiURI      string
	DownloadURI string
	MinPartSize int
	opts        *b2Options
}

// Update replaces the B2 object with a new one, in-place.
func (b *B2) Update(n *B2) {
	b.accountID = n.accountID
	b.authToken = n.authToken
	b.apiURI = n.apiURI
	b.DownloadURI = n.DownloadURI
	b.MinPartSize = n.MinPartSize
	b.opts = n.opts
}

type httpReply struct {
	resp *http.Response
	err  error
}

func makeNetRequest(req *http.Request, rt http.RoundTripper) <-chan httpReply {
	ch := make(chan httpReply)
	go func() {
		resp, err := rt.RoundTrip(req)
		ch <- httpReply{resp, err}
		close(ch)
	}()
	return ch
}

type requestBody struct {
	size int64
	body io.Reader
}

func (rb *requestBody) getSize() int64 {
	if rb == nil {
		return 0
	}
	return rb.size
}

func (rb *requestBody) getBody() io.Reader {
	if rb == nil {
		return nil
	}
	return rb.body
}

type keepFinalBytes struct {
	r      io.Reader
	remain int
	sha    [40]byte
}

func (k *keepFinalBytes) Read(p []byte) (int, error) {
	n, err := k.r.Read(p)
	if k.remain-n > 40 {
		k.remain -= n
		return n, err
	}
	// This was a whole lot harder than it looks.
	pi := -40 + k.remain
	if pi < 0 {
		pi = 0
	}
	pe := n
	ki := 40 - k.remain
	if ki < 0 {
		ki = 0
	}
	ke := n - k.remain + 40
	copy(k.sha[ki:ke], p[pi:pe])
	k.remain -= n
	return n, err
}

var reqID int64

func (o *b2Options) makeRequest(ctx context.Context, method, verb, uri string, b2req, b2resp interface{}, headers map[string]string, body *requestBody) error {
	var args []byte
	if b2req != nil {
		enc, err := json.Marshal(b2req)
		if err != nil {
			return err
		}
		args = enc
		body = &requestBody{
			body: bytes.NewBuffer(enc),
			size: int64(len(enc)),
		}
	}
	req, err := http.NewRequest(verb, uri, body.getBody())
	if err != nil {
		return err
	}
	req.ContentLength = body.getSize()
	for k, v := range headers {
		if strings.HasPrefix(k, "X-Bz-Info") || strings.HasPrefix(k, "X-Bz-File-Name") {
			v = escape(v)
		}
		req.Header.Set(k, v)
	}
	req.Header.Set("User-Agent", o.getUserAgent())
	req.Header.Set("X-Blazer-Request-ID", fmt.Sprintf("%d", atomic.AddInt64(&reqID, 1)))
	req.Header.Set("X-Blazer-Method", method)
	if o.failSomeUploads {
		req.Header.Add("X-Bz-Test-Mode", "fail_some_uploads")
	}
	if o.expireTokens {
		req.Header.Add("X-Bz-Test-Mode", "expire_some_account_authorization_tokens")
	}
	if o.capExceeded {
		req.Header.Add("X-Bz-Test-Mode", "force_cap_exceeded")
	}
	cancel := make(chan struct{})
	req.Cancel = cancel
	logRequest(req, args)
	ch := makeNetRequest(req, o.getTransport())
	var reply httpReply
	select {
	case reply = <-ch:
	case <-ctx.Done():
		close(cancel)
		return ctx.Err()
	}
	if reply.err != nil {
		// Connection errors are retryable.
		blog.V(2).Infof(">> %s uri: %v err: %v", method, req.URL, reply.err)
		return b2err{
			msg:   reply.err.Error(),
			retry: 1,
		}
	}
	resp := reply.resp
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return mkErr(resp)
	}
	var replyArgs []byte
	if b2resp != nil {
		rbuf := &bytes.Buffer{}
		r := io.TeeReader(resp.Body, rbuf)
		decoder := json.NewDecoder(r)
		if err := decoder.Decode(b2resp); err != nil {
			return err
		}
		replyArgs = rbuf.Bytes()
	} else {
		replyArgs, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
	}
	logResponse(resp, replyArgs)
	return nil
}

// AuthorizeAccount wraps b2_authorize_account.
func AuthorizeAccount(ctx context.Context, account, key string, opts ...AuthOption) (*B2, error) {
	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", account, key)))
	b2resp := &b2types.AuthorizeAccountResponse{}
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Basic %s", auth),
	}
	b2opts := &b2Options{}
	for _, f := range opts {
		f(b2opts)
	}
	if err := b2opts.makeRequest(ctx, "b2_authorize_account", "GET", b2opts.getAPIBase()+b2types.V1api+"b2_authorize_account", nil, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &B2{
		accountID:   b2resp.AccountID,
		authToken:   b2resp.AuthToken,
		apiURI:      b2resp.URI,
		DownloadURI: b2resp.DownloadURI,
		MinPartSize: b2resp.MinPartSize,
		opts:        b2opts,
	}, nil
}

// An AuthOption allows callers to choose per-session settings.
type AuthOption func(*b2Options)

// UserAgent sets the User-Agent HTTP header.  The default header is
// "blazer/<version>"; the value set here will be prepended to that.  This can
// be set multiple times.
func UserAgent(agent string) AuthOption {
	return func(o *b2Options) {
		if o.userAgent == "" {
			o.userAgent = agent
			return
		}
		o.userAgent = fmt.Sprintf("%s %s", agent, o.userAgent)
	}
}

// Transport returns an AuthOption that sets the underlying HTTP mechanism.
func Transport(rt http.RoundTripper) AuthOption {
	return func(o *b2Options) {
		o.transport = rt
	}
}

// FailSomeUploads requests intermittent upload failures from the B2 service.
// This is mostly useful for testing.
func FailSomeUploads() AuthOption {
	return func(o *b2Options) {
		o.failSomeUploads = true
	}
}

// ExpireSomeAuthTokens requests intermittent authentication failures from the
// B2 service.
func ExpireSomeAuthTokens() AuthOption {
	return func(o *b2Options) {
		o.expireTokens = true
	}
}

// ForceCapExceeded requests a cap limit from the B2 service.  This causes all
// uploads to be treated as if they would exceed the configure B2 capacity.
func ForceCapExceeded() AuthOption {
	return func(o *b2Options) {
		o.capExceeded = true
	}
}

type LifecycleRule struct {
	Prefix                 string
	DaysNewUntilHidden     int
	DaysHiddenUntilDeleted int
}

// CreateBucket wraps b2_create_bucket.
func (b *B2) CreateBucket(ctx context.Context, name, btype string, info map[string]string, rules []LifecycleRule) (*Bucket, error) {
	if btype != "allPublic" {
		btype = "allPrivate"
	}
	var b2rules []b2types.LifecycleRule
	for _, rule := range rules {
		b2rules = append(b2rules, b2types.LifecycleRule{
			Prefix:                 rule.Prefix,
			DaysNewUntilHidden:     rule.DaysNewUntilHidden,
			DaysHiddenUntilDeleted: rule.DaysHiddenUntilDeleted,
		})
	}
	b2req := &b2types.CreateBucketRequest{
		AccountID:      b.accountID,
		Name:           name,
		Type:           btype,
		Info:           info,
		LifecycleRules: b2rules,
	}
	b2resp := &b2types.CreateBucketResponse{}
	headers := map[string]string{
		"Authorization": b.authToken,
	}
	if err := b.opts.makeRequest(ctx, "b2_create_bucket", "POST", b.apiURI+b2types.V1api+"b2_create_bucket", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	var respRules []LifecycleRule
	for _, rule := range b2resp.LifecycleRules {
		respRules = append(respRules, LifecycleRule{
			Prefix:                 rule.Prefix,
			DaysNewUntilHidden:     rule.DaysNewUntilHidden,
			DaysHiddenUntilDeleted: rule.DaysHiddenUntilDeleted,
		})
	}
	return &Bucket{
		Name:           name,
		Info:           b2resp.Info,
		LifecycleRules: respRules,
		id:             b2resp.BucketID,
		rev:            b2resp.Revision,
		b2:             b,
	}, nil
}

// DeleteBucket wraps b2_delete_bucket.
func (b *Bucket) DeleteBucket(ctx context.Context) error {
	b2req := &b2types.DeleteBucketRequest{
		AccountID: b.b2.accountID,
		BucketID:  b.id,
	}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	return b.b2.opts.makeRequest(ctx, "b2_delete_bucket", "POST", b.b2.apiURI+b2types.V1api+"b2_delete_bucket", b2req, nil, headers, nil)
}

// Bucket holds B2 bucket details.
type Bucket struct {
	Name           string
	Type           string
	Info           map[string]string
	LifecycleRules []LifecycleRule
	id             string
	rev            int
	b2             *B2
}

// Update wraps b2_update_bucket.
func (b *Bucket) Update(ctx context.Context) (*Bucket, error) {
	var rules []b2types.LifecycleRule
	for _, rule := range b.LifecycleRules {
		rules = append(rules, b2types.LifecycleRule{
			DaysNewUntilHidden:     rule.DaysNewUntilHidden,
			DaysHiddenUntilDeleted: rule.DaysHiddenUntilDeleted,
			Prefix:                 rule.Prefix,
		})
	}
	b2req := &b2types.UpdateBucketRequest{
		AccountID: b.b2.accountID,
		BucketID:  b.id,
		// Name:           b.Name,
		Type:           b.Type,
		Info:           b.Info,
		LifecycleRules: rules,
		IfRevisionIs:   b.rev,
	}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	b2resp := &b2types.UpdateBucketResponse{}
	if err := b.b2.opts.makeRequest(ctx, "b2_update_bucket", "POST", b.b2.apiURI+b2types.V1api+"b2_update_bucket", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	var respRules []LifecycleRule
	for _, rule := range b2resp.LifecycleRules {
		respRules = append(respRules, LifecycleRule{
			Prefix:                 rule.Prefix,
			DaysNewUntilHidden:     rule.DaysNewUntilHidden,
			DaysHiddenUntilDeleted: rule.DaysHiddenUntilDeleted,
		})
	}
	return &Bucket{
		Name:           b.Name,
		Type:           b2resp.Type,
		Info:           b2resp.Info,
		LifecycleRules: respRules,
		id:             b2resp.BucketID,
		b2:             b.b2,
	}, nil
}

// BaseURL returns the base part of the download URLs.
func (b *Bucket) BaseURL() string {
	return b.b2.DownloadURI
}

// ListBuckets wraps b2_list_buckets.
func (b *B2) ListBuckets(ctx context.Context) ([]*Bucket, error) {
	b2req := &b2types.ListBucketsRequest{
		AccountID: b.accountID,
	}
	b2resp := &b2types.ListBucketsResponse{}
	headers := map[string]string{
		"Authorization": b.authToken,
	}
	if err := b.opts.makeRequest(ctx, "b2_list_buckets", "POST", b.apiURI+b2types.V1api+"b2_list_buckets", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	var buckets []*Bucket
	for _, bucket := range b2resp.Buckets {
		var rules []LifecycleRule
		for _, rule := range bucket.LifecycleRules {
			rules = append(rules, LifecycleRule{
				Prefix:                 rule.Prefix,
				DaysNewUntilHidden:     rule.DaysNewUntilHidden,
				DaysHiddenUntilDeleted: rule.DaysHiddenUntilDeleted,
			})
		}
		buckets = append(buckets, &Bucket{
			Name:           bucket.Name,
			Type:           bucket.Type,
			Info:           bucket.Info,
			LifecycleRules: rules,
			id:             bucket.BucketID,
			rev:            bucket.Revision,
			b2:             b,
		})
	}
	return buckets, nil
}

// URL holds information from the b2_get_upload_url API.
type URL struct {
	uri    string
	token  string
	b2     *B2
	bucket *Bucket
}

// Reload reloads URL in-place, by reissuing a b2_get_upload_url and
// overwriting the previous values.
func (url *URL) Reload(ctx context.Context) error {
	n, err := url.bucket.GetUploadURL(ctx)
	if err != nil {
		return err
	}
	url.uri = n.uri
	url.token = n.token
	return nil
}

// GetUploadURL wraps b2_get_upload_url.
func (b *Bucket) GetUploadURL(ctx context.Context) (*URL, error) {
	b2req := &b2types.GetUploadURLRequest{
		BucketID: b.id,
	}
	b2resp := &b2types.GetUploadURLResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := b.b2.opts.makeRequest(ctx, "b2_get_upload_url", "POST", b.b2.apiURI+b2types.V1api+"b2_get_upload_url", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &URL{
		uri:    b2resp.URI,
		token:  b2resp.Token,
		b2:     b.b2,
		bucket: b,
	}, nil
}

// File represents a B2 file.
type File struct {
	Name      string
	Size      int64
	Status    string
	Timestamp time.Time
	Info      *FileInfo
	id        string
	b2        *B2
}

// File returns a bare File struct, but with the appropriate id and b2
// interfaces.
func (b *Bucket) File(id, name string) *File {
	return &File{id: id, b2: b.b2, Name: name}
}

// UploadFile wraps b2_upload_file.
func (u *URL) UploadFile(ctx context.Context, r io.Reader, size int, name, contentType, sha1 string, info map[string]string) (*File, error) {
	headers := map[string]string{
		"Authorization":     u.token,
		"X-Bz-File-Name":    name,
		"Content-Type":      contentType,
		"Content-Length":    fmt.Sprintf("%d", size),
		"X-Bz-Content-Sha1": sha1,
	}
	for k, v := range info {
		headers[fmt.Sprintf("X-Bz-Info-%s", k)] = v
	}
	b2resp := &b2types.UploadFileResponse{}
	if err := u.b2.opts.makeRequest(ctx, "b2_upload_file", "POST", u.uri, nil, b2resp, headers, &requestBody{body: r, size: int64(size)}); err != nil {
		return nil, err
	}
	return &File{
		Name:      name,
		Size:      int64(size),
		Timestamp: millitime(b2resp.Timestamp),
		Status:    b2resp.Action,
		id:        b2resp.FileID,
		b2:        u.b2,
	}, nil
}

// DeleteFileVersion wraps b2_delete_file_version.
func (f *File) DeleteFileVersion(ctx context.Context) error {
	b2req := &b2types.DeleteFileVersionRequest{
		Name:   f.Name,
		FileID: f.id,
	}
	headers := map[string]string{
		"Authorization": f.b2.authToken,
	}
	return f.b2.opts.makeRequest(ctx, "b2_delete_file_version", "POST", f.b2.apiURI+b2types.V1api+"b2_delete_file_version", b2req, nil, headers, nil)
}

// LargeFile holds information necessary to implement B2 large file support.
type LargeFile struct {
	ID          string
	Timestamp   time.Time
	Name        string
	ContentType string
	Info        map[string]string

	b2 *B2

	mu     sync.Mutex
	size   int64
	hashes map[int]string
}

// StartLargeFile wraps b2_start_large_file.
func (b *Bucket) StartLargeFile(ctx context.Context, name, contentType string, info map[string]string) (*LargeFile, error) {
	b2req := &b2types.StartLargeFileRequest{
		BucketID:    b.id,
		Name:        name,
		ContentType: contentType,
		Info:        info,
	}
	b2resp := &b2types.StartLargeFileResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := b.b2.opts.makeRequest(ctx, "b2_start_large_file", "POST", b.b2.apiURI+b2types.V1api+"b2_start_large_file", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &LargeFile{
		ID:     b2resp.ID,
		b2:     b.b2,
		hashes: make(map[int]string),
	}, nil
}

// ListUnfinishedLargeFiles - lists all the unfinied large files.
func (b *Bucket) ListUnfinishedLargeFiles(ctx context.Context, continuation string, count int) ([]*LargeFile, string, error) {
	b2req := &b2types.ListUnfinishedLargeFilesRequest{
		BucketID:     b.id,
		Continuation: continuation,
		Count:        count,
	}
	b2resp := &b2types.ListUnfinishedLargeFilesResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := b.b2.opts.makeRequest(ctx, "b2_list_unfinished_large_files", "POST", b.b2.apiURI+b2types.V1api+"b2_list_unfinished_large_files",
		b2req, b2resp, headers, nil); err != nil {
		return nil, "", err
	}
	cont := b2resp.NextID
	var largeFiles []*LargeFile
	for _, f := range b2resp.Files {
		largeFiles = append(largeFiles, &LargeFile{
			ID:          f.ID,
			Timestamp:   millitime(f.Timestamp),
			Name:        f.Name,
			Info:        f.Info,
			ContentType: f.ContentType,
			b2:          b.b2,
			hashes:      make(map[int]string),
		})
	}
	return largeFiles, cont, nil
}

// CancelLargeFile wraps b2_cancel_large_file.
func (l *LargeFile) CancelLargeFile(ctx context.Context) error {
	b2req := &b2types.CancelLargeFileRequest{
		ID: l.ID,
	}
	headers := map[string]string{
		"Authorization": l.b2.authToken,
	}
	return l.b2.opts.makeRequest(ctx, "b2_cancel_large_file", "POST", l.b2.apiURI+b2types.V1api+"b2_cancel_large_file", b2req, nil, headers, nil)
}

// FilePart is a piece of a started, but not finished, large file upload.
type FilePart struct {
	Number int
	SHA1   string
	Size   int64
}

// ListParts wraps b2_list_parts.
func (f *File) ListParts(ctx context.Context, next, count int) ([]*FilePart, int, error) {
	b2req := &b2types.ListPartsRequest{
		ID:    f.id,
		Start: next,
		Count: count,
	}
	b2resp := &b2types.ListPartsResponse{}
	headers := map[string]string{
		"Authorization": f.b2.authToken,
	}
	if err := f.b2.opts.makeRequest(ctx, "b2_list_parts", "POST", f.b2.apiURI+b2types.V1api+"b2_list_parts", b2req, b2resp, headers, nil); err != nil {
		return nil, 0, err
	}
	var parts []*FilePart
	for _, part := range b2resp.Parts {
		parts = append(parts, &FilePart{
			Number: part.Number,
			SHA1:   part.SHA1,
			Size:   part.Size,
		})
	}
	return parts, b2resp.Next, nil
}

// CompileParts returns a LargeFile that can accept new data.  Seen is a
// mapping of completed part numbers to SHA1 strings; size is the total size of
// all the completed parts to this point.
func (f *File) CompileParts(size int64, seen map[int]string) *LargeFile {
	s := make(map[int]string)
	for k, v := range seen {
		s[k] = v
	}
	return &LargeFile{
		ID:     f.id,
		b2:     f.b2,
		size:   size,
		hashes: s,
	}
}

// FileChunk holds information necessary for uploading file chunks.
type FileChunk struct {
	url   string
	token string
	file  *LargeFile
}

type getUploadPartURLRequest struct {
	ID string `json:"fileId"`
}

type getUploadPartURLResponse struct {
	URL   string `json:"uploadUrl"`
	Token string `json:"authorizationToken"`
}

// GetUploadPartURL wraps b2_get_upload_part_url.
func (l *LargeFile) GetUploadPartURL(ctx context.Context) (*FileChunk, error) {
	b2req := &getUploadPartURLRequest{
		ID: l.ID,
	}
	b2resp := &getUploadPartURLResponse{}
	headers := map[string]string{
		"Authorization": l.b2.authToken,
	}
	if err := l.b2.opts.makeRequest(ctx, "b2_get_upload_part_url", "POST", l.b2.apiURI+b2types.V1api+"b2_get_upload_part_url", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &FileChunk{
		url:   b2resp.URL,
		token: b2resp.Token,
		file:  l,
	}, nil
}

// Reload reloads FileChunk in-place.
func (fc *FileChunk) Reload(ctx context.Context) error {
	n, err := fc.file.GetUploadPartURL(ctx)
	if err != nil {
		return err
	}
	fc.url = n.url
	fc.token = n.token
	return nil
}

// UploadPart wraps b2_upload_part.
func (fc *FileChunk) UploadPart(ctx context.Context, r io.Reader, sha1 string, size, index int) (string, error) {
	headers := map[string]string{
		"Authorization":     fc.token,
		"X-Bz-Part-Number":  fmt.Sprintf("%d", index),
		"Content-Length":    fmt.Sprintf("%d", size),
		"X-Bz-Content-Sha1": sha1,
	}
	b2resp := &b2types.UploadPartResponse{}
	if sha1 == "hex_digits_at_end" {
		r = &keepFinalBytes{r: r, remain: size}
	}
	if err := fc.file.b2.opts.makeRequest(ctx, "b2_upload_part", "POST", fc.url, nil, b2resp, headers, &requestBody{body: r, size: int64(size)}); err != nil {
		return "", err
	}
	fc.file.mu.Lock()
	if sha1 == "hex_digits_at_end" {
		sha1 = string(r.(*keepFinalBytes).sha[:])
	}
	fc.file.hashes[index] = sha1
	fc.file.size += int64(size)
	fc.file.mu.Unlock()
	return b2resp.SHA1, nil
}

// FinishLargeFile wraps b2_finish_large_file.
func (l *LargeFile) FinishLargeFile(ctx context.Context) (*File, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b2req := &b2types.FinishLargeFileRequest{
		ID:     l.ID,
		Hashes: make([]string, len(l.hashes)),
	}
	b2resp := &b2types.FinishLargeFileResponse{}
	for k, v := range l.hashes {
		b2req.Hashes[k-1] = v
	}
	headers := map[string]string{
		"Authorization": l.b2.authToken,
	}
	if err := l.b2.opts.makeRequest(ctx, "b2_finish_large_file", "POST", l.b2.apiURI+b2types.V1api+"b2_finish_large_file", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &File{
		Name:      b2resp.Name,
		Size:      l.size,
		Timestamp: millitime(b2resp.Timestamp),
		Status:    b2resp.Action,
		id:        b2resp.FileID,
		b2:        l.b2,
	}, nil
}

// ListFileNames wraps b2_list_file_names.
func (b *Bucket) ListFileNames(ctx context.Context, count int, continuation, prefix, delimiter string) ([]*File, string, error) {
	b2req := &b2types.ListFileNamesRequest{
		Count:        count,
		Continuation: continuation,
		BucketID:     b.id,
		Prefix:       prefix,
		Delimiter:    delimiter,
	}
	b2resp := &b2types.ListFileNamesResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := b.b2.opts.makeRequest(ctx, "b2_list_file_names", "POST", b.b2.apiURI+b2types.V1api+"b2_list_file_names", b2req, b2resp, headers, nil); err != nil {
		return nil, "", err
	}
	cont := b2resp.Continuation
	var files []*File
	for _, f := range b2resp.Files {
		files = append(files, &File{
			Name:      f.Name,
			Size:      f.Size,
			Status:    f.Action,
			Timestamp: millitime(f.Timestamp),
			Info: &FileInfo{
				Name:        f.Name,
				SHA1:        f.SHA1,
				Size:        f.Size,
				ContentType: f.ContentType,
				Info:        f.Info,
				Status:      f.Action,
				Timestamp:   millitime(f.Timestamp),
			},
			id: f.FileID,
			b2: b.b2,
		})
	}
	return files, cont, nil
}

// ListFileVersions wraps b2_list_file_versions.
func (b *Bucket) ListFileVersions(ctx context.Context, count int, startName, startID, prefix, delimiter string) ([]*File, string, string, error) {
	b2req := &b2types.ListFileVersionsRequest{
		BucketID:  b.id,
		Count:     count,
		StartName: startName,
		StartID:   startID,
		Prefix:    prefix,
		Delimiter: delimiter,
	}
	b2resp := &b2types.ListFileVersionsResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := b.b2.opts.makeRequest(ctx, "b2_list_file_versions", "POST", b.b2.apiURI+b2types.V1api+"b2_list_file_versions", b2req, b2resp, headers, nil); err != nil {
		return nil, "", "", err
	}
	var files []*File
	for _, f := range b2resp.Files {
		files = append(files, &File{
			Name:      f.Name,
			Size:      f.Size,
			Status:    f.Action,
			Timestamp: millitime(f.Timestamp),
			Info: &FileInfo{
				Name:        f.Name,
				SHA1:        f.SHA1,
				Size:        f.Size,
				ContentType: f.ContentType,
				Info:        f.Info,
				Status:      f.Action,
				Timestamp:   millitime(f.Timestamp),
			},
			id: f.FileID,
			b2: b.b2,
		})
	}
	return files, b2resp.NextName, b2resp.NextID, nil
}

// GetDownloadAuthorization wraps b2_get_download_authorization.
func (b *Bucket) GetDownloadAuthorization(ctx context.Context, prefix string, valid time.Duration) (string, error) {
	b2req := &b2types.GetDownloadAuthorizationRequest{
		BucketID: b.id,
		Prefix:   prefix,
		Valid:    int(valid.Seconds()),
	}
	b2resp := &b2types.GetDownloadAuthorizationResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := b.b2.opts.makeRequest(ctx, "b2_get_download_authorization", "POST", b.b2.apiURI+b2types.V1api+"b2_get_download_authorization", b2req, b2resp, headers, nil); err != nil {
		return "", err
	}
	return b2resp.Token, nil
}

// FileReader is an io.ReadCloser that downloads a file from B2.
type FileReader struct {
	io.ReadCloser
	ContentLength int
	ContentType   string
	SHA1          string
	ID            string
	Info          map[string]string
}

func mkRange(offset, size int64) string {
	if offset == 0 && size == 0 {
		return ""
	}
	if size == 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
}

// DownloadFileByName wraps b2_download_file_by_name.
func (b *Bucket) DownloadFileByName(ctx context.Context, name string, offset, size int64) (*FileReader, error) {
	uri := fmt.Sprintf("%s/file/%s/%s", b.b2.DownloadURI, b.Name, name)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", b.b2.authToken)
	req.Header.Set("X-Blazer-Request-ID", fmt.Sprintf("%d", atomic.AddInt64(&reqID, 1)))
	req.Header.Set("X-Blazer-Method", "b2_download_file_by_name")
	rng := mkRange(offset, size)
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	cancel := make(chan struct{})
	req.Cancel = cancel
	logRequest(req, nil)
	ch := makeNetRequest(req, b.b2.opts.getTransport())
	var reply httpReply
	select {
	case reply = <-ch:
	case <-ctx.Done():
		close(cancel)
		return nil, ctx.Err()
	}
	if reply.err != nil {
		return nil, reply.err
	}
	resp := reply.resp
	logResponse(resp, nil)
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		defer resp.Body.Close()
		return nil, mkErr(resp)
	}
	clen, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	info := make(map[string]string)
	for key := range resp.Header {
		if !strings.HasPrefix(key, "X-Bz-Info-") {
			continue
		}
		name, err := unescape(strings.TrimPrefix(key, "X-Bz-Info-"))
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		val, err := unescape(resp.Header.Get(key))
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		info[name] = val
	}
	return &FileReader{
		ReadCloser:    resp.Body,
		SHA1:          resp.Header.Get("X-Bz-Content-Sha1"),
		ID:            resp.Header.Get("X-Bz-File-Id"),
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: int(clen),
		Info:          info,
	}, nil
}

// HideFile wraps b2_hide_file.
func (b *Bucket) HideFile(ctx context.Context, name string) (*File, error) {
	b2req := &b2types.HideFileRequest{
		BucketID: b.id,
		File:     name,
	}
	b2resp := &b2types.HideFileResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := b.b2.opts.makeRequest(ctx, "b2_hide_file", "POST", b.b2.apiURI+b2types.V1api+"b2_hide_file", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &File{
		Status:    b2resp.Action,
		Name:      name,
		Timestamp: millitime(b2resp.Timestamp),
		b2:        b.b2,
		id:        b2resp.ID,
	}, nil
}

// FileInfo holds information about a specific file.
type FileInfo struct {
	Name        string
	SHA1        string
	ID          string
	Size        int64
	ContentType string
	Info        map[string]string
	Status      string
	Timestamp   time.Time
}

// GetFileInfo wraps b2_get_file_info.
func (f *File) GetFileInfo(ctx context.Context) (*FileInfo, error) {
	b2req := &b2types.GetFileInfoRequest{
		ID: f.id,
	}
	b2resp := &b2types.GetFileInfoResponse{}
	headers := map[string]string{
		"Authorization": f.b2.authToken,
	}
	if err := f.b2.opts.makeRequest(ctx, "b2_get_file_info", "POST", f.b2.apiURI+b2types.V1api+"b2_get_file_info", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	f.Status = b2resp.Action
	f.Name = b2resp.Name
	f.Timestamp = millitime(b2resp.Timestamp)
	f.Info = &FileInfo{
		Name:        b2resp.Name,
		SHA1:        b2resp.SHA1,
		Size:        b2resp.Size,
		ContentType: b2resp.ContentType,
		Info:        b2resp.Info,
		Status:      b2resp.Action,
		ID:          b2resp.FileID,
		Timestamp:   millitime(b2resp.Timestamp),
	}
	return f.Info, nil
}
