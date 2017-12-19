// Package oss implements functions for access oss service.
// It has two main struct Client and Bucket.
package oss

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"time"
)

//
// Client Sdk的入口，Client的方法可以完成bucket的各种操作，如create/delete bucket，
// set/get acl/lifecycle/referer/logging/website等。文件(object)的上传下载通过Bucket完成。
// 用户用oss.New创建Client。
//
type (
	// Client oss client
	Client struct {
		Config *Config // Oss Client configure
		Conn   *Conn   // Send http request
	}

	// ClientOption client option such as UseCname, Timeout, SecurityToken.
	ClientOption func(*Client)
)

//
// New 生成一个新的Client。
//
// endpoint        用户Bucket所在数据中心的访问域名，如http://oss-cn-hangzhou.aliyuncs.com。
// accessKeyId     用户标识。
// accessKeySecret 用户密钥。
//
// Client 生成的新Client。error为nil时有效。
// error  操作无错误时为nil，非nil时表示操作出错。
//
func New(endpoint, accessKeyID, accessKeySecret string, options ...ClientOption) (*Client, error) {
	// configuration
	config := getDefaultOssConfig()
	config.Endpoint = endpoint
	config.AccessKeyID = accessKeyID
	config.AccessKeySecret = accessKeySecret

	// url parse
	url := &urlMaker{}
	url.Init(config.Endpoint, config.IsCname, config.IsUseProxy)

	// http connect
	conn := &Conn{config: config, url: url}

	// oss client
	client := &Client{
		config,
		conn,
	}

	// client options parse
	for _, option := range options {
		option(client)
	}

	// create http connect
	err := conn.init(config, url)

	return client, err
}

//
// Bucket 取存储空间（Bucket）的对象实例。
//
// bucketName 存储空间名称。
// Bucket     新的Bucket。error为nil时有效。
//
// error 操作无错误时返回nil，非nil为错误信息。
//
func (client Client) Bucket(bucketName string) (*Bucket, error) {
	return &Bucket{
		client,
		bucketName,
	}, nil
}

//
// CreateBucket 创建Bucket。
//
// bucketName bucket名称，在整个OSS中具有全局唯一性，且不能修改。bucket名称的只能包括小写字母，数字和短横线-，
// 必须以小写字母或者数字开头，长度必须在3-255字节之间。
// options  创建bucket的选项。您可以使用选项ACL，指定bucket的访问权限。Bucket有以下三种访问权限，私有读写（ACLPrivate）、
// 公共读私有写（ACLPublicRead），公共读公共写(ACLPublicReadWrite)，默认访问权限是私有读写。可以使用StorageClass选项设置bucket的存储方式，目前支持：标准存储模式（StorageStandard）、 低频存储模式（StorageIA）、 归档存储模式（StorageArchive）。
//
// error 操作无错误时返回nil，非nil为错误信息。
//
func (client Client) CreateBucket(bucketName string, options ...Option) error {
	headers := make(map[string]string)
	handleOptions(headers, options)

	buffer := new(bytes.Buffer)

	isOptSet, val, _ := isOptionSet(options, storageClass)
	if isOptSet {
		cbConfig := createBucketConfiguration{StorageClass: val.(StorageClassType)}
		bs, err := xml.Marshal(cbConfig)
		if err != nil {
			return err
		}
		buffer.Write(bs)

		contentType := http.DetectContentType(buffer.Bytes())
		headers[HTTPHeaderContentType] = contentType
	}

	params := map[string]interface{}{}
	resp, err := client.do("PUT", bucketName, params, headers, buffer)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// ListBuckets 获取当前用户下的bucket。
//
// options 指定ListBuckets的筛选行为，Prefix、Marker、MaxKeys三个选项。Prefix限定前缀。
// Marker设定从Marker之后的第一个开始返回。MaxKeys限定此次返回的最大数目，默认为100。
// 常用使用场景的实现，参数示例程序list_bucket.go。
// ListBucketsResponse 操作成功后的返回值，error为nil时该返回值有效。
//
// error 操作无错误时返回nil，非nil为错误信息。
//
func (client Client) ListBuckets(options ...Option) (ListBucketsResult, error) {
	var out ListBucketsResult

	params, err := getRawParams(options)
	if err != nil {
		return out, err
	}

	resp, err := client.do("GET", "", params, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// IsBucketExist Bucket是否存在。
//
// bucketName 存储空间名称。
//
// bool  存储空间是否存在。error为nil时有效。
// error 操作无错误时返回nil，非nil为错误信息。
//
func (client Client) IsBucketExist(bucketName string) (bool, error) {
	listRes, err := client.ListBuckets(Prefix(bucketName), MaxKeys(1))
	if err != nil {
		return false, err
	}

	if len(listRes.Buckets) == 1 && listRes.Buckets[0].Name == bucketName {
		return true, nil
	}
	return false, nil
}

//
// DeleteBucket 删除空存储空间。非空时请先清理Object、Upload。
//
// bucketName 存储空间名称。
//
// error 操作无错误时返回nil，非nil为错误信息。
//
func (client Client) DeleteBucket(bucketName string) error {
	params := map[string]interface{}{}
	resp, err := client.do("DELETE", bucketName, params, nil, nil)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//
// GetBucketLocation 查看Bucket所属数据中心位置的信息。
//
// 如果您想了解"访问域名和数据中心"详细信息，请参看
// https://help.aliyun.com/document_detail/oss/user_guide/oss_concept/endpoint.html
//
// bucketName 存储空间名称。
//
// string Bucket所属的数据中心位置信息。
// error  操作无错误时返回nil，非nil为错误信息。
//
func (client Client) GetBucketLocation(bucketName string) (string, error) {
	params := map[string]interface{}{}
	params["location"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var LocationConstraint string
	err = xmlUnmarshal(resp.Body, &LocationConstraint)
	return LocationConstraint, err
}

//
// SetBucketACL 修改Bucket的访问权限。
//
// bucketName 存储空间名称。
// bucketAcl  bucket的访问权限。Bucket有以下三种访问权限，Bucket有以下三种访问权限，私有读写（ACLPrivate）、
// 公共读私有写（ACLPublicRead），公共读公共写(ACLPublicReadWrite)。
//
// error 操作无错误时返回nil，非nil为错误信息。
//
func (client Client) SetBucketACL(bucketName string, bucketACL ACLType) error {
	headers := map[string]string{HTTPHeaderOssACL: string(bucketACL)}
	params := map[string]interface{}{}
	resp, err := client.do("PUT", bucketName, params, headers, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// GetBucketACL 获得Bucket的访问权限。
//
// bucketName 存储空间名称。
//
// GetBucketAclResponse 操作成功后的返回值，error为nil时该返回值有效。
// error 操作无错误时返回nil，非nil为错误信息。
//
func (client Client) GetBucketACL(bucketName string) (GetBucketACLResult, error) {
	var out GetBucketACLResult
	params := map[string]interface{}{}
	params["acl"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// SetBucketLifecycle 修改Bucket的生命周期设置。
//
// OSS提供Object生命周期管理来为用户管理对象。用户可以为某个Bucket定义生命周期配置，来为该Bucket的Object定义各种规则。
// Bucket的拥有者可以通过SetBucketLifecycle来设置Bucket的Lifecycle配置。Lifecycle开启后，OSS将按照配置，
// 定期自动删除与Lifecycle规则相匹配的Object。如果您想了解更多的生命周期的信息，请参看
// https://help.aliyun.com/document_detail/oss/user_guide/manage_object/object_lifecycle.html
//
// bucketName 存储空间名称。
// rules 生命周期规则列表。生命周期规则有两种格式，指定绝对和相对过期时间，分布由days和year/month/day控制。
// 具体用法请参考示例程序sample/bucket_lifecycle.go。
//
// error 操作无错误时返回error为nil，非nil为错误信息。
//
func (client Client) SetBucketLifecycle(bucketName string, rules []LifecycleRule) error {
	lxml := lifecycleXML{Rules: convLifecycleRule(rules)}
	bs, err := xml.Marshal(lxml)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["lifecycle"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// DeleteBucketLifecycle 删除Bucket的生命周期设置。
//
//
// bucketName 存储空间名称。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) DeleteBucketLifecycle(bucketName string) error {
	params := map[string]interface{}{}
	params["lifecycle"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//
// GetBucketLifecycle 查看Bucket的生命周期设置。
//
// bucketName 存储空间名称。
//
// GetBucketLifecycleResponse 操作成功的返回值，error为nil时该返回值有效。Rules为该bucket上的规则列表。
// error 操作无错误时为nil，非nil为错误信息。
//
func (client Client) GetBucketLifecycle(bucketName string) (GetBucketLifecycleResult, error) {
	var out GetBucketLifecycleResult
	params := map[string]interface{}{}
	params["lifecycle"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// SetBucketReferer 设置bucket的referer访问白名单和是否允许referer字段为空的请求访问。
//
// 防止用户在OSS上的数据被其他人盗用，OSS支持基于HTTP header中表头字段referer的防盗链方法。可以通过OSS控制台或者API的方式对
// 一个bucket设置referer字段的白名单和是否允许referer字段为空的请求访问。例如，对于一个名为oss-example的bucket，
// 设置其referer白名单为http://www.aliyun.com。则所有referer为http://www.aliyun.com的请求才能访问oss-example
// 这个bucket中的object。如果您还需要了解更多信息，请参看
// https://help.aliyun.com/document_detail/oss/user_guide/security_management/referer.html
//
// bucketName  存储空间名称。
// referers  访问白名单列表。一个bucket可以支持多个referer参数。referer参数支持通配符"*"和"?"。
// 用法请参看示例sample/bucket_referer.go
// allowEmptyReferer  指定是否允许referer字段为空的请求访问。 默认为true。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) SetBucketReferer(bucketName string, referers []string, allowEmptyReferer bool) error {
	rxml := RefererXML{}
	rxml.AllowEmptyReferer = allowEmptyReferer
	if referers == nil {
		rxml.RefererList = append(rxml.RefererList, "")
	} else {
		for _, referer := range referers {
			rxml.RefererList = append(rxml.RefererList, referer)
		}
	}

	bs, err := xml.Marshal(rxml)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["referer"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// GetBucketReferer 获得Bucket的白名单地址。
//
// bucketName 存储空间名称。
//
// GetBucketRefererResponse 操作成功的返回值，error为nil时该返回值有效。
// error 操作无错误时为nil，非nil为错误信息。
//
func (client Client) GetBucketReferer(bucketName string) (GetBucketRefererResult, error) {
	var out GetBucketRefererResult
	params := map[string]interface{}{}
	params["referer"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// SetBucketLogging 修改Bucket的日志设置。
//
// OSS为您提供自动保存访问日志记录功能。Bucket的拥有者可以开启访问日志记录功能。当一个bucket开启访问日志记录功能后，
// OSS自动将访问这个bucket的请求日志，以小时为单位，按照固定的命名规则，生成一个Object写入用户指定的bucket中。
// 如果您需要更多，请参看 https://help.aliyun.com/document_detail/oss/user_guide/security_management/logging.html
//
// bucketName   需要记录访问日志的Bucket。
// targetBucket 访问日志记录到的Bucket。
// targetPrefix bucketName中需要存储访问日志记录的object前缀。为空记录所有object的访问日志。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) SetBucketLogging(bucketName, targetBucket, targetPrefix string,
	isEnable bool) error {
	var err error
	var bs []byte
	if isEnable {
		lxml := LoggingXML{}
		lxml.LoggingEnabled.TargetBucket = targetBucket
		lxml.LoggingEnabled.TargetPrefix = targetPrefix
		bs, err = xml.Marshal(lxml)
	} else {
		lxml := loggingXMLEmpty{}
		bs, err = xml.Marshal(lxml)
	}

	if err != nil {
		return err
	}

	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["logging"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// DeleteBucketLogging 删除Bucket的日志设置。
//
// bucketName 需要删除访问日志的Bucket。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) DeleteBucketLogging(bucketName string) error {
	params := map[string]interface{}{}
	params["logging"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//
// GetBucketLogging 获得Bucket的日志设置。
//
// bucketName  需要删除访问日志的Bucket。
// GetBucketLoggingResponse  操作成功的返回值，error为nil时该返回值有效。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) GetBucketLogging(bucketName string) (GetBucketLoggingResult, error) {
	var out GetBucketLoggingResult
	params := map[string]interface{}{}
	params["logging"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// SetBucketWebsite 设置/修改Bucket的默认首页以及错误页。
//
// OSS支持静态网站托管，Website操作可以将一个bucket设置成静态网站托管模式 。您可以将自己的Bucket配置成静态网站托管模式。
// 如果您需要更多，请参看 https://help.aliyun.com/document_detail/oss/user_guide/static_host_website.html
//
// bucketName     需要设置Website的Bucket。
// indexDocument  索引文档。
// errorDocument  错误文档。
//
// error  操作无错误为nil，非nil为错误信息。
//
func (client Client) SetBucketWebsite(bucketName, indexDocument, errorDocument string) error {
	wxml := WebsiteXML{}
	wxml.IndexDocument.Suffix = indexDocument
	wxml.ErrorDocument.Key = errorDocument

	bs, err := xml.Marshal(wxml)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := make(map[string]string)
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// DeleteBucketWebsite 删除Bucket的Website设置。
//
// bucketName  需要删除website设置的Bucket。
//
// error  操作无错误为nil，非nil为错误信息。
//
func (client Client) DeleteBucketWebsite(bucketName string) error {
	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//
// GetBucketWebsite 获得Bucket的默认首页以及错误页。
//
// bucketName 存储空间名称。
//
// GetBucketWebsiteResponse 操作成功的返回值，error为nil时该返回值有效。
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) GetBucketWebsite(bucketName string) (GetBucketWebsiteResult, error) {
	var out GetBucketWebsiteResult
	params := map[string]interface{}{}
	params["website"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// SetBucketCORS 设置Bucket的跨域访问(CORS)规则。
//
// 跨域访问的更多信息，请参看 https://help.aliyun.com/document_detail/oss/user_guide/security_management/cors.html
//
// bucketName 需要设置Website的Bucket。
// corsRules  待设置的CORS规则。用法请参看示例代码sample/bucket_cors.go。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) SetBucketCORS(bucketName string, corsRules []CORSRule) error {
	corsxml := CORSXML{}
	for _, v := range corsRules {
		cr := CORSRule{}
		cr.AllowedMethod = v.AllowedMethod
		cr.AllowedOrigin = v.AllowedOrigin
		cr.AllowedHeader = v.AllowedHeader
		cr.ExposeHeader = v.ExposeHeader
		cr.MaxAgeSeconds = v.MaxAgeSeconds
		corsxml.CORSRules = append(corsxml.CORSRules, cr)
	}

	bs, err := xml.Marshal(corsxml)
	if err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	headers := map[string]string{}
	headers[HTTPHeaderContentType] = contentType

	params := map[string]interface{}{}
	params["cors"] = nil
	resp, err := client.do("PUT", bucketName, params, headers, buffer)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// DeleteBucketCORS 删除Bucket的Website设置。
//
// bucketName 需要删除cors设置的Bucket。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) DeleteBucketCORS(bucketName string) error {
	params := map[string]interface{}{}
	params["cors"] = nil
	resp, err := client.do("DELETE", bucketName, params, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//
// GetBucketCORS 获得Bucket的CORS设置。
//
//
// bucketName  存储空间名称。
// GetBucketCORSResult  操作成功的返回值，error为nil时该返回值有效。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) GetBucketCORS(bucketName string) (GetBucketCORSResult, error) {
	var out GetBucketCORSResult
	params := map[string]interface{}{}
	params["cors"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// GetBucketInfo 获得Bucket的信息。
//
// bucketName  存储空间名称。
// GetBucketInfoResult  操作成功的返回值，error为nil时该返回值有效。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (client Client) GetBucketInfo(bucketName string) (GetBucketInfoResult, error) {
	var out GetBucketInfoResult
	params := map[string]interface{}{}
	params["bucketInfo"] = nil
	resp, err := client.do("GET", bucketName, params, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// UseCname 设置是否使用CNAME，默认不使用。
//
// isUseCname true设置endpoint格式是cname格式，false为非cname格式，默认false
//
func UseCname(isUseCname bool) ClientOption {
	return func(client *Client) {
		client.Config.IsCname = isUseCname
		client.Conn.url.Init(client.Config.Endpoint, client.Config.IsCname, client.Config.IsUseProxy)
	}
}

//
// Timeout 设置HTTP超时时间。
//
// connectTimeoutSec HTTP链接超时时间，单位是秒，默认10秒。0表示永不超时。
// readWriteTimeout  HTTP发送接受数据超时时间，单位是秒，默认20秒。0表示永不超时。
//
func Timeout(connectTimeoutSec, readWriteTimeout int64) ClientOption {
	return func(client *Client) {
		client.Config.HTTPTimeout.ConnectTimeout =
			time.Second * time.Duration(connectTimeoutSec)
		client.Config.HTTPTimeout.ReadWriteTimeout =
			time.Second * time.Duration(readWriteTimeout)
		client.Config.HTTPTimeout.HeaderTimeout =
			time.Second * time.Duration(readWriteTimeout)
		client.Config.HTTPTimeout.LongTimeout =
			time.Second * time.Duration(readWriteTimeout*10)
	}
}

//
// SecurityToken 临时用户设置SecurityToken。
//
// token STS token
//
func SecurityToken(token string) ClientOption {
	return func(client *Client) {
		client.Config.SecurityToken = strings.TrimSpace(token)
	}
}

//
// EnableMD5 是否启用MD5校验，默认启用。
//
// isEnableMD5 true启用MD5校验，false不启用MD5校验
//
func EnableMD5(isEnableMD5 bool) ClientOption {
	return func(client *Client) {
		client.Config.IsEnableMD5 = isEnableMD5
	}
}

//
// MD5ThresholdCalcInMemory 使用内存计算MD5值的上限，默认16MB。
//
// threshold 单位Byte。上传内容小于threshold在MD5在内存中计算，大于使用临时文件计算MD5
//
func MD5ThresholdCalcInMemory(threshold int64) ClientOption {
	return func(client *Client) {
		client.Config.MD5Threshold = threshold
	}
}

//
// EnableCRC 上传是否启用CRC校验，默认启用。
//
// isEnableCRC true启用CRC校验，false不启用CRC校验
//
func EnableCRC(isEnableCRC bool) ClientOption {
	return func(client *Client) {
		client.Config.IsEnableCRC = isEnableCRC
	}
}

//
// UserAgent 指定UserAgent，默认如下aliyun-sdk-go/1.2.0 (windows/-/amd64;go1.5.2)。
//
// userAgent user agent字符串。
//
func UserAgent(userAgent string) ClientOption {
	return func(client *Client) {
		client.Config.UserAgent = userAgent
	}
}

//
// Proxy 设置代理服务器，默认不使用代理。
//
// proxyHost 代理服务器地址，格式是host或host:port
//
func Proxy(proxyHost string) ClientOption {
	return func(client *Client) {
		client.Config.IsUseProxy = true
		client.Config.ProxyHost = proxyHost
		client.Conn.url.Init(client.Config.Endpoint, client.Config.IsCname, client.Config.IsUseProxy)
	}
}

//
// AuthProxy 设置需要认证的代理服务器，默认不使用代理。
//
// proxyHost 代理服务器地址，格式是host或host:port
// proxyUser 代理服务器认证的用户名
// proxyPassword 代理服务器认证的用户密码
//
func AuthProxy(proxyHost, proxyUser, proxyPassword string) ClientOption {
	return func(client *Client) {
		client.Config.IsUseProxy = true
		client.Config.ProxyHost = proxyHost
		client.Config.IsAuthProxy = true
		client.Config.ProxyUser = proxyUser
		client.Config.ProxyPassword = proxyPassword
		client.Conn.url.Init(client.Config.Endpoint, client.Config.IsCname, client.Config.IsUseProxy)
	}
}

// Private
func (client Client) do(method, bucketName string, params map[string]interface{},
	headers map[string]string, data io.Reader) (*Response, error) {
	return client.Conn.Do(method, bucketName, "", params,
		headers, data, 0, nil)
}
