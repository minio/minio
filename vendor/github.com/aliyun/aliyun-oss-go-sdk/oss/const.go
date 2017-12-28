package oss

import "os"

// ACLType Bucket/Object的访问控制
type ACLType string

const (
	// ACLPrivate 私有读写
	ACLPrivate ACLType = "private"

	// ACLPublicRead 公共读私有写
	ACLPublicRead ACLType = "public-read"

	// ACLPublicReadWrite 公共读写
	ACLPublicReadWrite ACLType = "public-read-write"

	// ACLDefault Object默认权限，Bucket无此权限
	ACLDefault ACLType = "default"
)

// MetadataDirectiveType 对象COPY时新对象是否使用原对象的Meta
type MetadataDirectiveType string

const (
	// MetaCopy 目标对象使用源对象的META
	MetaCopy MetadataDirectiveType = "COPY"

	// MetaReplace 目标对象使用自定义的META
	MetaReplace MetadataDirectiveType = "REPLACE"
)

// StorageClassType Bucket的存储类型
type StorageClassType string

const (
	// StorageStandard 标准存储模式
	StorageStandard StorageClassType = "Standard"

	// StorageIA 低频存储模式
	StorageIA StorageClassType = "IA"

	// StorageArchive 归档存储模式
	StorageArchive StorageClassType = "Archive"
)

// HTTPMethod HTTP请求方法
type HTTPMethod string

const (
	// HTTPGet HTTP请求方法 GET
	HTTPGet HTTPMethod = "GET"

	// HTTPPut HTTP请求方法 PUT
	HTTPPut HTTPMethod = "PUT"

	// HTTPHead HTTP请求方法 HEAD
	HTTPHead HTTPMethod = "HEAD"

	// HTTPPost HTTP请求方法 POST
	HTTPPost HTTPMethod = "POST"

	// HTTPDelete HTTP请求方法 DELETE
	HTTPDelete HTTPMethod = "DELETE"
)

// Http头标签
const (
	HTTPHeaderAcceptEncoding     string = "Accept-Encoding"
	HTTPHeaderAuthorization             = "Authorization"
	HTTPHeaderCacheControl              = "Cache-Control"
	HTTPHeaderContentDisposition        = "Content-Disposition"
	HTTPHeaderContentEncoding           = "Content-Encoding"
	HTTPHeaderContentLength             = "Content-Length"
	HTTPHeaderContentMD5                = "Content-MD5"
	HTTPHeaderContentType               = "Content-Type"
	HTTPHeaderContentLanguage           = "Content-Language"
	HTTPHeaderDate                      = "Date"
	HTTPHeaderEtag                      = "ETag"
	HTTPHeaderExpires                   = "Expires"
	HTTPHeaderHost                      = "Host"
	HTTPHeaderLastModified              = "Last-Modified"
	HTTPHeaderRange                     = "Range"
	HTTPHeaderLocation                  = "Location"
	HTTPHeaderOrigin                    = "Origin"
	HTTPHeaderServer                    = "Server"
	HTTPHeaderUserAgent                 = "User-Agent"
	HTTPHeaderIfModifiedSince           = "If-Modified-Since"
	HTTPHeaderIfUnmodifiedSince         = "If-Unmodified-Since"
	HTTPHeaderIfMatch                   = "If-Match"
	HTTPHeaderIfNoneMatch               = "If-None-Match"

	HTTPHeaderOssACL                         = "X-Oss-Acl"
	HTTPHeaderOssMetaPrefix                  = "X-Oss-Meta-"
	HTTPHeaderOssObjectACL                   = "X-Oss-Object-Acl"
	HTTPHeaderOssSecurityToken               = "X-Oss-Security-Token"
	HTTPHeaderOssServerSideEncryption        = "X-Oss-Server-Side-Encryption"
	HTTPHeaderOssCopySource                  = "X-Oss-Copy-Source"
	HTTPHeaderOssCopySourceRange             = "X-Oss-Copy-Source-Range"
	HTTPHeaderOssCopySourceIfMatch           = "X-Oss-Copy-Source-If-Match"
	HTTPHeaderOssCopySourceIfNoneMatch       = "X-Oss-Copy-Source-If-None-Match"
	HTTPHeaderOssCopySourceIfModifiedSince   = "X-Oss-Copy-Source-If-Modified-Since"
	HTTPHeaderOssCopySourceIfUnmodifiedSince = "X-Oss-Copy-Source-If-Unmodified-Since"
	HTTPHeaderOssMetadataDirective           = "X-Oss-Metadata-Directive"
	HTTPHeaderOssNextAppendPosition          = "X-Oss-Next-Append-Position"
	HTTPHeaderOssRequestID                   = "X-Oss-Request-Id"
	HTTPHeaderOssCRC64                       = "X-Oss-Hash-Crc64ecma"
	HTTPHeaderOssSymlinkTarget               = "X-Oss-Symlink-Target"
)

// Http Param
const (
	HTTPParamExpires       = "Expires"
	HTTPParamAccessKeyID   = "OSSAccessKeyId"
	HTTPParamSignature     = "Signature"
	HTTPParamSecurityToken = "security-token"
)

// 其它常量
const (
	MaxPartSize = 5 * 1024 * 1024 * 1024 // 文件片最大值，5GB
	MinPartSize = 100 * 1024             // 文件片最小值，100KBß

	FilePermMode = os.FileMode(0664) // 新建文件默认权限

	TempFilePrefix = "oss-go-temp-" // 临时文件前缀
	TempFileSuffix = ".temp"        // 临时文件后缀

	CheckpointFileSuffix = ".cp" // Checkpoint文件后缀

	Version = "1.7.0" // Go sdk版本
)
