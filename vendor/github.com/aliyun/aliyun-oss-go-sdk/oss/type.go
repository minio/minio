package oss

import (
	"encoding/xml"
	"net/url"
	"time"
)

// ListBucketsResult ListBuckets请求返回的结果
type ListBucketsResult struct {
	XMLName     xml.Name           `xml:"ListAllMyBucketsResult"`
	Prefix      string             `xml:"Prefix"`         // 本次查询结果的前缀
	Marker      string             `xml:"Marker"`         // 标明查询的起点，未全部返回时有此节点
	MaxKeys     int                `xml:"MaxKeys"`        // 返回结果的最大数目，未全部返回时有此节点
	IsTruncated bool               `xml:"IsTruncated"`    // 所有的结果是否已经全部返回
	NextMarker  string             `xml:"NextMarker"`     // 表示下一次查询的起点
	Owner       Owner              `xml:"Owner"`          // 拥有者信息
	Buckets     []BucketProperties `xml:"Buckets>Bucket"` // Bucket列表
}

// BucketProperties Bucket信息
type BucketProperties struct {
	XMLName      xml.Name  `xml:"Bucket"`
	Name         string    `xml:"Name"`         // Bucket名称
	Location     string    `xml:"Location"`     // Bucket所在的数据中心
	CreationDate time.Time `xml:"CreationDate"` // Bucket创建时间
	StorageClass string    `xml:"StorageClass"` // Bucket的存储方式
}

// GetBucketACLResult GetBucketACL请求返回的结果
type GetBucketACLResult struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	ACL     string   `xml:"AccessControlList>Grant"` // Bucket权限
	Owner   Owner    `xml:"Owner"`                   // Bucket拥有者信息
}

// LifecycleConfiguration Bucket的Lifecycle配置
type LifecycleConfiguration struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rules   []LifecycleRule `xml:"Rule"`
}

// LifecycleRule Lifecycle规则
type LifecycleRule struct {
	XMLName    xml.Name            `xml:"Rule"`
	ID         string              `xml:"ID"`         // 规则唯一的ID
	Prefix     string              `xml:"Prefix"`     // 规则所适用Object的前缀
	Status     string              `xml:"Status"`     // 规则是否生效
	Expiration LifecycleExpiration `xml:"Expiration"` // 规则的过期属性
}

// LifecycleExpiration 规则的过期属性
type LifecycleExpiration struct {
	XMLName xml.Name  `xml:"Expiration"`
	Days    int       `xml:"Days,omitempty"` // 最后修改时间过后多少天生效
	Date    time.Time `xml:"Date,omitempty"` // 指定规则何时生效
}

type lifecycleXML struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rules   []lifecycleRule `xml:"Rule"`
}

type lifecycleRule struct {
	XMLName    xml.Name            `xml:"Rule"`
	ID         string              `xml:"ID"`
	Prefix     string              `xml:"Prefix"`
	Status     string              `xml:"Status"`
	Expiration lifecycleExpiration `xml:"Expiration"`
}

type lifecycleExpiration struct {
	XMLName xml.Name `xml:"Expiration"`
	Days    int      `xml:"Days,omitempty"`
	Date    string   `xml:"Date,omitempty"`
}

const expirationDateFormat = "2006-01-02T15:04:05.000Z"

func convLifecycleRule(rules []LifecycleRule) []lifecycleRule {
	rs := []lifecycleRule{}
	for _, rule := range rules {
		r := lifecycleRule{}
		r.ID = rule.ID
		r.Prefix = rule.Prefix
		r.Status = rule.Status
		if rule.Expiration.Date.IsZero() {
			r.Expiration.Days = rule.Expiration.Days
		} else {
			r.Expiration.Date = rule.Expiration.Date.Format(expirationDateFormat)
		}
		rs = append(rs, r)
	}
	return rs
}

// BuildLifecycleRuleByDays 指定过期天数构建Lifecycle规则
func BuildLifecycleRuleByDays(id, prefix string, status bool, days int) LifecycleRule {
	var statusStr = "Enabled"
	if !status {
		statusStr = "Disabled"
	}
	return LifecycleRule{ID: id, Prefix: prefix, Status: statusStr,
		Expiration: LifecycleExpiration{Days: days}}
}

// BuildLifecycleRuleByDate 指定过期时间构建Lifecycle规则
func BuildLifecycleRuleByDate(id, prefix string, status bool, year, month, day int) LifecycleRule {
	var statusStr = "Enabled"
	if !status {
		statusStr = "Disabled"
	}
	date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return LifecycleRule{ID: id, Prefix: prefix, Status: statusStr,
		Expiration: LifecycleExpiration{Date: date}}
}

// GetBucketLifecycleResult GetBucketLifecycle请求请求结果
type GetBucketLifecycleResult LifecycleConfiguration

// RefererXML Referer配置
type RefererXML struct {
	XMLName           xml.Name `xml:"RefererConfiguration"`
	AllowEmptyReferer bool     `xml:"AllowEmptyReferer"`   // 是否允许referer字段为空的请求访问
	RefererList       []string `xml:"RefererList>Referer"` // referer访问白名单
}

// GetBucketRefererResult GetBucketReferer请教返回结果
type GetBucketRefererResult RefererXML

// LoggingXML Logging配置
type LoggingXML struct {
	XMLName        xml.Name       `xml:"BucketLoggingStatus"`
	LoggingEnabled LoggingEnabled `xml:"LoggingEnabled"` // 访问日志信息容器
}

type loggingXMLEmpty struct {
	XMLName xml.Name `xml:"BucketLoggingStatus"`
}

// LoggingEnabled 访问日志信息容器
type LoggingEnabled struct {
	XMLName      xml.Name `xml:"LoggingEnabled"`
	TargetBucket string   `xml:"TargetBucket"` //存放访问日志的Bucket
	TargetPrefix string   `xml:"TargetPrefix"` //保存访问日志的文件前缀
}

// GetBucketLoggingResult GetBucketLogging请求返回结果
type GetBucketLoggingResult LoggingXML

// WebsiteXML Website配置
type WebsiteXML struct {
	XMLName       xml.Name      `xml:"WebsiteConfiguration"`
	IndexDocument IndexDocument `xml:"IndexDocument"` // 目录URL时添加的索引文件
	ErrorDocument ErrorDocument `xml:"ErrorDocument"` // 404错误时使用的文件
}

// IndexDocument 目录URL时添加的索引文件
type IndexDocument struct {
	XMLName xml.Name `xml:"IndexDocument"`
	Suffix  string   `xml:"Suffix"` // 目录URL时添加的索引文件名
}

// ErrorDocument 404错误时使用的文件
type ErrorDocument struct {
	XMLName xml.Name `xml:"ErrorDocument"`
	Key     string   `xml:"Key"` // 404错误时使用的文件名
}

// GetBucketWebsiteResult GetBucketWebsite请求返回结果
type GetBucketWebsiteResult WebsiteXML

// CORSXML CORS配置
type CORSXML struct {
	XMLName   xml.Name   `xml:"CORSConfiguration"`
	CORSRules []CORSRule `xml:"CORSRule"` // CORS规则列表
}

// CORSRule CORS规则
type CORSRule struct {
	XMLName       xml.Name `xml:"CORSRule"`
	AllowedOrigin []string `xml:"AllowedOrigin"` // 允许的来源，默认通配符"*"
	AllowedMethod []string `xml:"AllowedMethod"` // 允许的方法
	AllowedHeader []string `xml:"AllowedHeader"` // 允许的请求头
	ExposeHeader  []string `xml:"ExposeHeader"`  // 允许的响应头
	MaxAgeSeconds int      `xml:"MaxAgeSeconds"` // 最大的缓存时间
}

// GetBucketCORSResult GetBucketCORS请求返回的结果
type GetBucketCORSResult CORSXML

// GetBucketInfoResult GetBucketInfo请求返回结果
type GetBucketInfoResult struct {
	XMLName    xml.Name   `xml:"BucketInfo"`
	BucketInfo BucketInfo `xml:"Bucket"`
}

// BucketInfo Bucket信息
type BucketInfo struct {
	XMLName          xml.Name  `xml:"Bucket"`
	Name             string    `xml:"Name"`                    // Bucket名称
	Location         string    `xml:"Location"`                // Bucket所在的数据中心
	CreationDate     time.Time `xml:"CreationDate"`            // Bucket创建时间
	ExtranetEndpoint string    `xml:"ExtranetEndpoint"`        // Bucket访问的外网域名
	IntranetEndpoint string    `xml:"IntranetEndpoint"`        // Bucket访问的内网域名
	ACL              string    `xml:"AccessControlList>Grant"` // Bucket权限
	Owner            Owner     `xml:"Owner"`                   // Bucket拥有者信息
	StorageClass     string    `xml:"StorageClass"`            // Bucket存储类型
}

// ListObjectsResult ListObjects请求返回结果
type ListObjectsResult struct {
	XMLName        xml.Name           `xml:"ListBucketResult"`
	Prefix         string             `xml:"Prefix"`                // 本次查询结果的开始前缀
	Marker         string             `xml:"Marker"`                // 这次查询的起点
	MaxKeys        int                `xml:"MaxKeys"`               // 请求返回结果的最大数目
	Delimiter      string             `xml:"Delimiter"`             // 对Object名字进行分组的字符
	IsTruncated    bool               `xml:"IsTruncated"`           // 是否所有的结果都已经返回
	NextMarker     string             `xml:"NextMarker"`            // 下一次查询的起点
	Objects        []ObjectProperties `xml:"Contents"`              // Object类别
	CommonPrefixes []string           `xml:"CommonPrefixes>Prefix"` // 以delimiter结尾并有共同前缀的Object的集合
}

// ObjectProperties Objecct属性
type ObjectProperties struct {
	XMLName      xml.Name  `xml:"Contents"`
	Key          string    `xml:"Key"`          // Object的Key
	Type         string    `xml:"Type"`         // Object Type
	Size         int64     `xml:"Size"`         // Object的长度字节数
	ETag         string    `xml:"ETag"`         // 标示Object的内容
	Owner        Owner     `xml:"Owner"`        // 保存Object拥有者信息的容器
	LastModified time.Time `xml:"LastModified"` // Object最后修改时间
	StorageClass string    `xml:"StorageClass"` // Object的存储类型
}

// Owner Bucket/Object的owner
type Owner struct {
	XMLName     xml.Name `xml:"Owner"`
	ID          string   `xml:"ID"`          // 用户ID
	DisplayName string   `xml:"DisplayName"` // Owner名字
}

// CopyObjectResult CopyObject请求返回的结果
type CopyObjectResult struct {
	XMLName      xml.Name  `xml:"CopyObjectResult"`
	LastModified time.Time `xml:"LastModified"` // 新Object最后更新时间
	ETag         string    `xml:"ETag"`         // 新Object的ETag值
}

// GetObjectACLResult GetObjectACL请求返回的结果
type GetObjectACLResult GetBucketACLResult

type deleteXML struct {
	XMLName xml.Name       `xml:"Delete"`
	Objects []DeleteObject `xml:"Object"` // 删除的所有Object
	Quiet   bool           `xml:"Quiet"`  // 安静响应模式
}

// DeleteObject 删除的Object
type DeleteObject struct {
	XMLName xml.Name `xml:"Object"`
	Key     string   `xml:"Key"` // Object名称
}

// DeleteObjectsResult DeleteObjects请求返回结果
type DeleteObjectsResult struct {
	XMLName        xml.Name `xml:"DeleteResult"`
	DeletedObjects []string `xml:"Deleted>Key"` // 删除的Object列表
}

// InitiateMultipartUploadResult InitiateMultipartUpload请求返回结果
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`   // Bucket名称
	Key      string   `xml:"Key"`      // 上传Object名称
	UploadID string   `xml:"UploadId"` // 生成的UploadId
}

// UploadPart 上传/拷贝的分片
type UploadPart struct {
	XMLName    xml.Name `xml:"Part"`
	PartNumber int      `xml:"PartNumber"` // Part编号
	ETag       string   `xml:"ETag"`       // ETag缓存码
}

type uploadParts []UploadPart

func (slice uploadParts) Len() int {
	return len(slice)
}

func (slice uploadParts) Less(i, j int) bool {
	return slice[i].PartNumber < slice[j].PartNumber
}

func (slice uploadParts) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// UploadPartCopyResult 拷贝分片请求返回的结果
type UploadPartCopyResult struct {
	XMLName      xml.Name  `xml:"CopyPartResult"`
	LastModified time.Time `xml:"LastModified"` // 最后修改时间
	ETag         string    `xml:"ETag"`         // ETag
}

type completeMultipartUploadXML struct {
	XMLName xml.Name     `xml:"CompleteMultipartUpload"`
	Part    []UploadPart `xml:"Part"`
}

// CompleteMultipartUploadResult 提交分片上传任务返回结果
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"` // Object的URL
	Bucket   string   `xml:"Bucket"`   // Bucket名称
	ETag     string   `xml:"ETag"`     // Object的ETag
	Key      string   `xml:"Key"`      // Object的名字
}

// ListUploadedPartsResult ListUploadedParts请求返回结果
type ListUploadedPartsResult struct {
	XMLName              xml.Name       `xml:"ListPartsResult"`
	Bucket               string         `xml:"Bucket"`               // Bucket名称
	Key                  string         `xml:"Key"`                  // Object名称
	UploadID             string         `xml:"UploadId"`             // 上传Id
	NextPartNumberMarker string         `xml:"NextPartNumberMarker"` // 下一个Part的位置
	MaxParts             int            `xml:"MaxParts"`             // 最大Part个数
	IsTruncated          bool           `xml:"IsTruncated"`          // 是否完全上传完成
	UploadedParts        []UploadedPart `xml:"Part"`                 // 已完成的Part
}

// UploadedPart 该任务已经上传的分片
type UploadedPart struct {
	XMLName      xml.Name  `xml:"Part"`
	PartNumber   int       `xml:"PartNumber"`   // Part编号
	LastModified time.Time `xml:"LastModified"` // 最后一次修改时间
	ETag         string    `xml:"ETag"`         // ETag缓存码
	Size         int       `xml:"Size"`         // Part大小
}

// ListMultipartUploadResult ListMultipartUpload请求返回结果
type ListMultipartUploadResult struct {
	XMLName            xml.Name            `xml:"ListMultipartUploadsResult"`
	Bucket             string              `xml:"Bucket"`                // Bucket名称
	Delimiter          string              `xml:"Delimiter"`             // 分组分割符
	Prefix             string              `xml:"Prefix"`                // 筛选前缀
	KeyMarker          string              `xml:"KeyMarker"`             // 起始Object位置
	UploadIDMarker     string              `xml:"UploadIdMarker"`        // 起始UploadId位置
	NextKeyMarker      string              `xml:"NextKeyMarker"`         // 如果没有全部返回，标明接下去的KeyMarker位置
	NextUploadIDMarker string              `xml:"NextUploadIdMarker"`    // 如果没有全部返回，标明接下去的UploadId位置
	MaxUploads         int                 `xml:"MaxUploads"`            // 返回最大Upload数目
	IsTruncated        bool                `xml:"IsTruncated"`           // 是否完全返回
	Uploads            []UncompletedUpload `xml:"Upload"`                // 未完成上传的MultipartUpload
	CommonPrefixes     []string            `xml:"CommonPrefixes>Prefix"` // 所有名字包含指定的前缀且第一次出现delimiter字符之间的object作为一组的分组结果
}

// UncompletedUpload 未完成的Upload任务
type UncompletedUpload struct {
	XMLName   xml.Name  `xml:"Upload"`
	Key       string    `xml:"Key"`       // Object名称
	UploadID  string    `xml:"UploadId"`  // 对应UploadId
	Initiated time.Time `xml:"Initiated"` // 初始化时间，格式2012-02-23T04:18:23.000Z
}

// 解析URL编码
func decodeDeleteObjectsResult(result *DeleteObjectsResult) error {
	var err error
	for i := 0; i < len(result.DeletedObjects); i++ {
		result.DeletedObjects[i], err = url.QueryUnescape(result.DeletedObjects[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// 解析URL编码
func decodeListObjectsResult(result *ListObjectsResult) error {
	var err error
	result.Prefix, err = url.QueryUnescape(result.Prefix)
	if err != nil {
		return err
	}
	result.Marker, err = url.QueryUnescape(result.Marker)
	if err != nil {
		return err
	}
	result.Delimiter, err = url.QueryUnescape(result.Delimiter)
	if err != nil {
		return err
	}
	result.NextMarker, err = url.QueryUnescape(result.NextMarker)
	if err != nil {
		return err
	}
	for i := 0; i < len(result.Objects); i++ {
		result.Objects[i].Key, err = url.QueryUnescape(result.Objects[i].Key)
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(result.CommonPrefixes); i++ {
		result.CommonPrefixes[i], err = url.QueryUnescape(result.CommonPrefixes[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// 解析URL编码
func decodeListMultipartUploadResult(result *ListMultipartUploadResult) error {
	var err error
	result.Prefix, err = url.QueryUnescape(result.Prefix)
	if err != nil {
		return err
	}
	result.Delimiter, err = url.QueryUnescape(result.Delimiter)
	if err != nil {
		return err
	}
	result.KeyMarker, err = url.QueryUnescape(result.KeyMarker)
	if err != nil {
		return err
	}
	result.NextKeyMarker, err = url.QueryUnescape(result.NextKeyMarker)
	if err != nil {
		return err
	}
	for i := 0; i < len(result.Uploads); i++ {
		result.Uploads[i].Key, err = url.QueryUnescape(result.Uploads[i].Key)
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(result.CommonPrefixes); i++ {
		result.CommonPrefixes[i], err = url.QueryUnescape(result.CommonPrefixes[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// createBucketConfiguration 规则的过期属性
type createBucketConfiguration struct {
	XMLName      xml.Name         `xml:"CreateBucketConfiguration"`
	StorageClass StorageClassType `xml:"StorageClass,omitempty"`
}
