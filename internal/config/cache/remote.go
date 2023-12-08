package cache

import (
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/minio/minio/internal/amztime"
	xhttp "github.com/minio/minio/internal/http"
)

//go:generate msgp -file=$GOFILE

// ObjectInfo represents the object information cached remotely
type ObjectInfo struct {
	Key        string    `json:"key"`
	Bucket     string    `json:"bucket"`
	ETag       string    `json:"etag"`
	ModTime    time.Time `json:"modTime"`
	StatusCode int       `json:"statusCode"`

	// Optional elements
	CacheControl string            `json:"cacheControl,omitempty" msg:",omitempty"`
	Expires      string            `json:"expires,omitempty" msg:",omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty" msg:",omitempty"`
	Range        string            `json:"range,omitempty" msg:",omitempty"`
	PartNumber   int               `json:"partNumber,omitempty" msg:",omitempty"`
	Size         int64             `json:"size,omitempty" msg:",omitempty"` // Full size of the object
	Data         []byte            `json:"data,omitempty" msg:",omitempty"` // Data can container full data of the object or partial
}

// WriteHeaders writes the response headers for conditional requests
func (oi ObjectInfo) WriteHeaders(w http.ResponseWriter, preamble, statusCode func()) {
	preamble()

	if !oi.ModTime.IsZero() {
		w.Header().Set(xhttp.LastModified, oi.ModTime.UTC().Format(http.TimeFormat))
	}

	if oi.ETag != "" {
		w.Header()[xhttp.ETag] = []string{"\"" + oi.ETag + "\""}
	}

	if oi.Expires != "" {
		w.Header().Set(xhttp.Expires, oi.Expires)
	}

	if oi.CacheControl != "" {
		w.Header().Set(xhttp.CacheControl, oi.CacheControl)
	}

	statusCode()
}

// CondCheck represents the conditional request made to the remote cache
// for validation during GET/HEAD object requests.
type CondCheck struct {
	ObjectInfo
	IfMatch           string     `json:"ifMatch,omitempty" msg:",omitempty"`
	IfNoneMatch       string     `json:"ifNoneMatch,omitempty" msg:",omitempty"`
	IfModifiedSince   *time.Time `json:"ifModSince,omitempty" msg:",omitempty"`
	IfUnModifiedSince *time.Time `json:"ifUnmodSince,omitempty" msg:",omitempty"`
	IfRange           string     `json:"ifRange,omitempty" msg:",omitempty"`
	IfPartNumber      int        `json:"ifPartNumber,omitempty" msg:",omitempty"`
}

// IsSet tells the cache lookup to avoid sending a request
func (r *CondCheck) IsSet() bool {
	if r == nil {
		return false
	}
	return r.IfMatch != "" || r.IfNoneMatch != "" || r.IfModifiedSince != nil || r.IfUnModifiedSince != nil || r.IfRange != ""
}

var etagRegex = regexp.MustCompile("\"*?([^\"]*?)\"*?$")

// canonicalizeETag returns ETag with leading and trailing double-quotes removed,
// if any present
func canonicalizeETag(etag string) string {
	return etagRegex.ReplaceAllString(etag, "$1")
}

// Init - populates the input values, initializes CondCheck
// before sending the request remotely.
func (r *CondCheck) Init(bucket, object string, header http.Header) {
	r.Key = object
	r.Bucket = bucket

	ifModifiedSinceHeader := header.Get(xhttp.IfModifiedSince)
	if ifModifiedSinceHeader != "" {
		if givenTime, err := amztime.ParseHeader(ifModifiedSinceHeader); err == nil {
			r.IfModifiedSince = &givenTime
		}
	}
	ifUnmodifiedSinceHeader := header.Get(xhttp.IfUnmodifiedSince)
	if ifUnmodifiedSinceHeader != "" {
		if givenTime, err := amztime.ParseHeader(ifUnmodifiedSinceHeader); err == nil {
			r.IfUnModifiedSince = &givenTime
		}
	}
	r.IfMatch = canonicalizeETag(header.Get(xhttp.IfMatch))
	r.IfNoneMatch = canonicalizeETag(header.Get(xhttp.IfNoneMatch))
	r.IfRange = header.Get(xhttp.Range)
	ifPartNumberHeader := header.Get(xhttp.PartNumber)
	if ifPartNumberHeader != "" {
		if partNumber, err := strconv.Atoi(ifPartNumberHeader); err == nil {
			r.IfPartNumber = partNumber
		}
	}
}
