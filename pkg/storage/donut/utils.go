package donut

import (
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// BucketACL - bucket level access control
type BucketACL string

// different types of ACL's currently supported for buckets
const (
	BucketPrivate         = BucketACL("private")
	BucketPublicRead      = BucketACL("public-read")
	BucketPublicReadWrite = BucketACL("public-read-write")
)

func (b BucketACL) String() string {
	return string(b)
}

// IsPrivate - is acl Private
func (b BucketACL) IsPrivate() bool {
	return b == BucketACL("private")
}

// IsPublicRead - is acl PublicRead
func (b BucketACL) IsPublicRead() bool {
	return b == BucketACL("public-read")
}

// IsPublicReadWrite - is acl PublicReadWrite
func (b BucketACL) IsPublicReadWrite() bool {
	return b == BucketACL("public-read-write")
}

// BucketMetadata - name and create date
type BucketMetadata struct {
	Name    string
	Created time.Time
	ACL     BucketACL
}

// ObjectMetadata - object key and its relevant metadata
type ObjectMetadata struct {
	Bucket string
	Key    string

	ContentType string
	Created     time.Time
	Md5         string
	Size        int64
}

// FilterMode type
type FilterMode int

// FilterMode list
const (
	DelimiterPrefixMode FilterMode = iota
	DelimiterMode
	PrefixMode
	DefaultMode
)

// PartMetadata - various types of individual part resources
type PartMetadata struct {
	PartNumber   int
	LastModified time.Time
	ETag         string
	Size         int64
}

// ObjectResourcesMetadata - various types of object resources
type ObjectResourcesMetadata struct {
	Bucket               string
	EncodingType         string
	Key                  string
	UploadID             string
	StorageClass         string
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	Part []*PartMetadata
}

// UploadMetadata container capturing metadata on in progress multipart upload in a given bucket
type UploadMetadata struct {
	Key          string
	UploadID     string
	StorageClass string
	Initiated    time.Time
}

// BucketMultipartResourcesMetadata - various types of bucket resources for inprogress multipart uploads
type BucketMultipartResourcesMetadata struct {
	KeyMarker          string
	UploadIDMarker     string
	NextKeyMarker      string
	NextUploadIDMarker string
	EncodingType       string
	MaxUploads         int
	IsTruncated        bool
	Upload             []*UploadMetadata
	Prefix             string
	Delimiter          string
	CommonPrefixes     []string
}

// BucketResourcesMetadata - various types of bucket resources
type BucketResourcesMetadata struct {
	Prefix         string
	Marker         string
	NextMarker     string
	Maxkeys        int
	EncodingType   string
	Delimiter      string
	IsTruncated    bool
	CommonPrefixes []string
	Mode           FilterMode
}

// GetMode - Populate filter mode
func GetMode(resources BucketResourcesMetadata) FilterMode {
	var f FilterMode
	switch true {
	case resources.Delimiter != "" && resources.Prefix != "":
		f = DelimiterPrefixMode
	case resources.Delimiter != "" && resources.Prefix == "":
		f = DelimiterMode
	case resources.Delimiter == "" && resources.Prefix != "":
		f = PrefixMode
	case resources.Delimiter == "" && resources.Prefix == "":
		f = DefaultMode
	}

	return f
}

// IsValidBucketACL - is provided acl string supported
func IsValidBucketACL(acl string) bool {
	switch acl {
	case "private":
		fallthrough
	case "public-read":
		fallthrough
	case "public-read-write":
		return true
	case "":
		// by default its "private"
		return true
	default:
		return false
	}
}

// IsDelimiterPrefixSet Delimiter and Prefix set
func (b BucketResourcesMetadata) IsDelimiterPrefixSet() bool {
	return b.Mode == DelimiterPrefixMode
}

// IsDelimiterSet Delimiter set
func (b BucketResourcesMetadata) IsDelimiterSet() bool {
	return b.Mode == DelimiterMode
}

// IsPrefixSet Prefix set
func (b BucketResourcesMetadata) IsPrefixSet() bool {
	return b.Mode == PrefixMode
}

// IsDefault No query values
func (b BucketResourcesMetadata) IsDefault() bool {
	return b.Mode == DefaultMode
}

// IsValidBucket - verify bucket name in accordance with
//  - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func IsValidBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if bucket[0] == '.' || bucket[len(bucket)-1] == '.' {
		return false
	}
	if match, _ := regexp.MatchString("\\.\\.", bucket); match == true {
		return false
	}
	// We don't support buckets with '.' in them
	match, _ := regexp.MatchString("^[a-zA-Z][a-zA-Z0-9\\-]+[a-zA-Z0-9]$", bucket)
	return match
}

// IsValidObjectName - verify object name in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func IsValidObjectName(object string) bool {
	if strings.TrimSpace(object) == "" {
		return true
	}
	if len(object) > 1024 || len(object) == 0 {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	return true
}
