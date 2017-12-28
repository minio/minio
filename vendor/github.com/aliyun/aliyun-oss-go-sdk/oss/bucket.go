package oss

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Bucket implements the operations of object.
type Bucket struct {
	Client     Client
	BucketName string
}

//
// PutObject 新建Object，如果Object已存在，覆盖原有Object。
//
// objectKey  上传对象的名称，使用UTF-8编码、长度必须在1-1023字节之间、不能以“/”或者“\”字符开头。
// reader     io.Reader读取object的数据。
// options    上传对象时可以指定对象的属性，可用选项有CacheControl、ContentDisposition、ContentEncoding、
// Expires、ServerSideEncryption、ObjectACL、Meta，具体含义请参看
// https://help.aliyun.com/document_detail/oss/api-reference/object/PutObject.html
//
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) PutObject(objectKey string, reader io.Reader, options ...Option) error {
	opts := addContentType(options, objectKey)

	request := &PutObjectRequest{
		ObjectKey: objectKey,
		Reader:    reader,
	}
	resp, err := bucket.DoPutObject(request, opts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return err
}

//
// PutObjectFromFile 新建Object，内容从本地文件中读取。
//
// objectKey 上传对象的名称。
// filePath  本地文件，上传对象的值为该文件内容。
// options   上传对象时可以指定对象的属性。详见PutObject的options。
//
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) PutObjectFromFile(objectKey, filePath string, options ...Option) error {
	fd, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	opts := addContentType(options, filePath, objectKey)

	request := &PutObjectRequest{
		ObjectKey: objectKey,
		Reader:    fd,
	}
	resp, err := bucket.DoPutObject(request, opts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return err
}

//
// DoPutObject 上传文件。
//
// request  上传请求。
// options  上传选项。
//
// Response 上传请求返回值。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) DoPutObject(request *PutObjectRequest, options []Option) (*Response, error) {
	isOptSet, _, _ := isOptionSet(options, HTTPHeaderContentType)
	if !isOptSet {
		options = addContentType(options, request.ObjectKey)
	}

	listener := getProgressListener(options)

	params := map[string]interface{}{}
	resp, err := bucket.do("PUT", request.ObjectKey, params, options, request.Reader, listener)
	if err != nil {
		return nil, err
	}

	if bucket.getConfig().IsEnableCRC {
		err = checkCRC(resp, "DoPutObject")
		if err != nil {
			return resp, err
		}
	}

	err = checkRespCode(resp.StatusCode, []int{http.StatusOK})

	return resp, err
}

//
// GetObject 下载文件。
//
// objectKey 下载的文件名称。
// options   对象的属性限制项，可选值有Range、IfModifiedSince、IfUnmodifiedSince、IfMatch、
// IfNoneMatch、AcceptEncoding，详细请参考
// https://help.aliyun.com/document_detail/oss/api-reference/object/GetObject.html
//
// io.ReadCloser  reader，读取数据后需要close。error为nil时有效。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) GetObject(objectKey string, options ...Option) (io.ReadCloser, error) {
	result, err := bucket.DoGetObject(&GetObjectRequest{objectKey}, options)
	if err != nil {
		return nil, err
	}
	return result.Response.Body, nil
}

//
// GetObjectToFile 下载文件。
//
// objectKey  下载的文件名称。
// filePath   下载对象的内容写到该本地文件。
// options    对象的属性限制项。详见GetObject的options。
//
// error  操作无错误时返回error为nil，非nil为错误说明。
//
func (bucket Bucket) GetObjectToFile(objectKey, filePath string, options ...Option) error {
	tempFilePath := filePath + TempFileSuffix

	// 读取Object内容
	result, err := bucket.DoGetObject(&GetObjectRequest{objectKey}, options)
	if err != nil {
		return err
	}
	defer result.Response.Body.Close()

	// 如果文件不存在则创建，存在则清空
	fd, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, FilePermMode)
	if err != nil {
		return err
	}

	// 存储数据到文件
	_, err = io.Copy(fd, result.Response.Body)
	fd.Close()
	if err != nil {
		return err
	}

	// 比较CRC值
	hasRange, _, _ := isOptionSet(options, HTTPHeaderRange)
	if bucket.getConfig().IsEnableCRC && !hasRange {
		result.Response.ClientCRC = result.ClientCRC.Sum64()
		err = checkCRC(result.Response, "GetObjectToFile")
		if err != nil {
			os.Remove(tempFilePath)
			return err
		}
	}

	return os.Rename(tempFilePath, filePath)
}

//
// DoGetObject 下载文件
//
// request 下载请求
// options    对象的属性限制项。详见GetObject的options。
//
// GetObjectResult 下载请求返回值。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) DoGetObject(request *GetObjectRequest, options []Option) (*GetObjectResult, error) {
	params := map[string]interface{}{}
	resp, err := bucket.do("GET", request.ObjectKey, params, options, nil, nil)
	if err != nil {
		return nil, err
	}

	result := &GetObjectResult{
		Response: resp,
	}

	// crc
	var crcCalc hash.Hash64
	hasRange, _, _ := isOptionSet(options, HTTPHeaderRange)
	if bucket.getConfig().IsEnableCRC && !hasRange {
		crcCalc = crc64.New(crcTable())
		result.ServerCRC = resp.ServerCRC
		result.ClientCRC = crcCalc
	}

	// progress
	listener := getProgressListener(options)

	contentLen, _ := strconv.ParseInt(resp.Headers.Get(HTTPHeaderContentLength), 10, 64)
	resp.Body = ioutil.NopCloser(TeeReader(resp.Body, crcCalc, contentLen, listener, nil))

	return result, nil
}

//
// CopyObject 同一个bucket内拷贝Object。
//
// srcObjectKey  Copy的源对象。
// destObjectKey Copy的目标对象。
// options  Copy对象时，您可以指定源对象的限制条件，满足限制条件时copy，不满足时返回错误，您可以选择如下选项CopySourceIfMatch、
// CopySourceIfNoneMatch、CopySourceIfModifiedSince、CopySourceIfUnmodifiedSince、MetadataDirective。
// Copy对象时，您可以指定目标对象的属性，如CacheControl、ContentDisposition、ContentEncoding、Expires、
// ServerSideEncryption、ObjectACL、Meta，选项的含义请参看
// https://help.aliyun.com/document_detail/oss/api-reference/object/CopyObject.html
//
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) CopyObject(srcObjectKey, destObjectKey string, options ...Option) (CopyObjectResult, error) {
	var out CopyObjectResult
	options = append(options, CopySource(bucket.BucketName, url.QueryEscape(srcObjectKey)))
	params := map[string]interface{}{}
	resp, err := bucket.do("PUT", destObjectKey, params, options, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// CopyObjectTo bucket间拷贝object。
//
// srcObjectKey   源Object名称。源Bucket名称为Bucket.BucketName。
// destBucketName  目标Bucket名称。
// destObjectKey  目标Object名称。
// options        Copy选项，详见CopyObject的options。
//
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) CopyObjectTo(destBucketName, destObjectKey, srcObjectKey string, options ...Option) (CopyObjectResult, error) {
	return bucket.copy(srcObjectKey, destBucketName, destObjectKey, options...)
}

//
// CopyObjectFrom bucket间拷贝object。
//
// srcBucketName  源Bucket名称。
// srcObjectKey   源Object名称。
// destObjectKey  目标Object名称。目标Bucket名称为Bucket.BucketName。
// options        Copy选项，详见CopyObject的options。
//
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) CopyObjectFrom(srcBucketName, srcObjectKey, destObjectKey string, options ...Option) (CopyObjectResult, error) {
	destBucketName := bucket.BucketName
	var out CopyObjectResult
	srcBucket, err := bucket.Client.Bucket(srcBucketName)
	if err != nil {
		return out, err
	}

	return srcBucket.copy(srcObjectKey, destBucketName, destObjectKey, options...)
}

func (bucket Bucket) copy(srcObjectKey, destBucketName, destObjectKey string, options ...Option) (CopyObjectResult, error) {
	var out CopyObjectResult
	options = append(options, CopySource(bucket.BucketName, url.QueryEscape(srcObjectKey)))
	headers := make(map[string]string)
	err := handleOptions(headers, options)
	if err != nil {
		return out, err
	}
	params := map[string]interface{}{}
	resp, err := bucket.Client.Conn.Do("PUT", destBucketName, destObjectKey, params, headers, nil, 0, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// AppendObject 追加方式上传。
//
// AppendObject参数必须包含position，其值指定从何处进行追加。首次追加操作的position必须为0，
// 后续追加操作的position是Object的当前长度。例如，第一次Append Object请求指定position值为0，
// content-length是65536；那么，第二次Append Object需要指定position为65536。
// 每次操作成功后，响应头部x-oss-next-append-position也会标明下一次追加的position。
//
// objectKey  需要追加的Object。
// reader     io.Reader，读取追的内容。
// appendPosition  object追加的起始位置。
// destObjectProperties  第一次追加时指定新对象的属性，如CacheControl、ContentDisposition、ContentEncoding、
// Expires、ServerSideEncryption、ObjectACL。
//
// int64 下次追加的开始位置，error为nil空时有效。
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) AppendObject(objectKey string, reader io.Reader, appendPosition int64, options ...Option) (int64, error) {
	request := &AppendObjectRequest{
		ObjectKey: objectKey,
		Reader:    reader,
		Position:  appendPosition,
	}

	result, err := bucket.DoAppendObject(request, options)
	if err != nil {
		return appendPosition, err
	}

	return result.NextPosition, err
}

//
// DoAppendObject 追加上传。
//
// request 追加上传请求。
// options 追加上传选项。
//
// AppendObjectResult 追加上传请求返回值。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) DoAppendObject(request *AppendObjectRequest, options []Option) (*AppendObjectResult, error) {
	params := map[string]interface{}{}
	params["append"] = nil
	params["position"] = strconv.FormatInt(request.Position, 10)
	headers := make(map[string]string)

	opts := addContentType(options, request.ObjectKey)
	handleOptions(headers, opts)

	var initCRC uint64
	isCRCSet, initCRCOpt, _ := isOptionSet(options, initCRC64)
	if isCRCSet {
		initCRC = initCRCOpt.(uint64)
	}

	listener := getProgressListener(options)

	handleOptions(headers, opts)
	resp, err := bucket.Client.Conn.Do("POST", bucket.BucketName, request.ObjectKey, params, headers,
		request.Reader, initCRC, listener)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	nextPosition, _ := strconv.ParseInt(resp.Headers.Get(HTTPHeaderOssNextAppendPosition), 10, 64)
	result := &AppendObjectResult{
		NextPosition: nextPosition,
		CRC:          resp.ServerCRC,
	}

	if bucket.getConfig().IsEnableCRC && isCRCSet {
		err = checkCRC(resp, "AppendObject")
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

//
// DeleteObject 删除Object。
//
// objectKey 待删除Object。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) DeleteObject(objectKey string) error {
	params := map[string]interface{}{}
	resp, err := bucket.do("DELETE", objectKey, params, nil, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//
// DeleteObjects 批量删除object。
//
// objectKeys 待删除object类表。
// options 删除选项，DeleteObjectsQuiet，是否是安静模式，默认不使用。
//
// DeleteObjectsResult 非安静模式的的返回值。
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) DeleteObjects(objectKeys []string, options ...Option) (DeleteObjectsResult, error) {
	out := DeleteObjectsResult{}
	dxml := deleteXML{}
	for _, key := range objectKeys {
		dxml.Objects = append(dxml.Objects, DeleteObject{Key: key})
	}
	isQuiet, _ := findOption(options, deleteObjectsQuiet, false)
	dxml.Quiet = isQuiet.(bool)

	bs, err := xml.Marshal(dxml)
	if err != nil {
		return out, err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	contentType := http.DetectContentType(buffer.Bytes())
	options = append(options, ContentType(contentType))
	sum := md5.Sum(bs)
	b64 := base64.StdEncoding.EncodeToString(sum[:])
	options = append(options, ContentMD5(b64))

	params := map[string]interface{}{}
	params["delete"] = nil
	params["encoding-type"] = "url"

	resp, err := bucket.do("POST", "", params, options, buffer, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	if !dxml.Quiet {
		if err = xmlUnmarshal(resp.Body, &out); err == nil {
			err = decodeDeleteObjectsResult(&out)
		}
	}
	return out, err
}

//
// IsObjectExist object是否存在。
//
// bool  object是否存在，true存在，false不存在。error为nil时有效。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) IsObjectExist(objectKey string) (bool, error) {
	_, err := bucket.GetObjectMeta(objectKey)
	if err == nil {
		return true, nil
	}

	switch err.(type) {
	case ServiceError:
		if err.(ServiceError).StatusCode == 404 && err.(ServiceError).Code == "NoSuchKey" {
			return false, nil
		}
	}

	return false, err
}

//
// ListObjects 获得Bucket下筛选后所有的object的列表。
//
// options  ListObject的筛选行为。Prefix指定的前缀、MaxKeys最大数目、Marker第一个开始、Delimiter对Object名字进行分组的字符。
//
// 您有如下8个object，my-object-1, my-object-11, my-object-2, my-object-21,
// my-object-22, my-object-3, my-object-31, my-object-32。如果您指定了Prefix为my-object-2,
// 则返回my-object-2, my-object-21, my-object-22三个object。如果您指定了Marker为my-object-22，
// 则返回my-object-3, my-object-31, my-object-32三个object。如果您指定MaxKeys则每次最多返回MaxKeys个，
// 最后一次可能不足。这三个参数可以组合使用，实现分页等功能。如果把prefix设为某个文件夹名，就可以罗列以此prefix开头的文件，
// 即该文件夹下递归的所有的文件和子文件夹。如果再把delimiter设置为"/"时，返回值就只罗列该文件夹下的文件，该文件夹下的子文件名
// 返回在CommonPrefixes部分，子文件夹下递归的文件和文件夹不被显示。例如一个bucket存在三个object，fun/test.jpg、
// fun/movie/001.avi、fun/movie/007.avi。若设定prefix为"fun/"，则返回三个object；如果增加设定
// delimiter为"/"，则返回文件"fun/test.jpg"和前缀"fun/movie/"，即实现了文件夹的逻辑。
//
// 常用场景，请参数示例sample/list_object.go。
//
// ListObjectsResponse  操作成功后的返回值，成员Objects为bucket中对象列表。error为nil时该返回值有效。
//
func (bucket Bucket) ListObjects(options ...Option) (ListObjectsResult, error) {
	var out ListObjectsResult

	options = append(options, EncodingType("url"))
	params, err := getRawParams(options)
	if err != nil {
		return out, err
	}

	resp, err := bucket.do("GET", "", params, nil, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	if err != nil {
		return out, err
	}

	err = decodeListObjectsResult(&out)
	return out, err
}

//
// SetObjectMeta 设置Object的Meta。
//
// objectKey object
// options 指定对象的属性，有以下可选项CacheControl、ContentDisposition、ContentEncoding、Expires、
// ServerSideEncryption、Meta。
//
// error 操作无错误时error为nil，非nil为错误信息。
//
func (bucket Bucket) SetObjectMeta(objectKey string, options ...Option) error {
	options = append(options, MetadataDirective(MetaReplace))
	_, err := bucket.CopyObject(objectKey, objectKey, options...)
	return err
}

//
// GetObjectDetailedMeta 查询Object的头信息。
//
// objectKey object名称。
// objectPropertyConstraints 对象的属性限制项，满足时正常返回，不满足时返回错误。现在项有IfModifiedSince、IfUnmodifiedSince、
// IfMatch、IfNoneMatch。具体含义请参看 https://help.aliyun.com/document_detail/oss/api-reference/object/HeadObject.html
//
// http.Header  对象的meta，error为nil时有效。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) GetObjectDetailedMeta(objectKey string, options ...Option) (http.Header, error) {
	params := map[string]interface{}{}
	resp, err := bucket.do("HEAD", objectKey, params, options, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp.Headers, nil
}

//
// GetObjectMeta 查询Object的头信息。
//
// GetObjectMeta相比GetObjectDetailedMeta更轻量，仅返回指定Object的少量基本meta信息，
// 包括该Object的ETag、Size（对象大小）、LastModified，其中Size由响应头Content-Length的数值表示。
//
// objectKey object名称。
//
// http.Header 对象的meta，error为nil时有效。
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) GetObjectMeta(objectKey string) (http.Header, error) {
	params := map[string]interface{}{}
	params["objectMeta"] = nil
	//resp, err := bucket.do("GET", objectKey, "?objectMeta", "", nil, nil, nil)
	resp, err := bucket.do("GET", objectKey, params, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp.Headers, nil
}

//
// SetObjectACL 修改Object的ACL权限。
//
// 只有Bucket Owner才有权限调用PutObjectACL来修改Object的ACL。Object ACL优先级高于Bucket ACL。
// 例如Bucket ACL是private的，而Object ACL是public-read-write的，则访问这个Object时，
// 先判断Object的ACL，所以所有用户都拥有这个Object的访问权限，即使这个Bucket是private bucket。
// 如果某个Object从来没设置过ACL，则访问权限遵循Bucket ACL。
//
// Object的读操作包括GetObject，HeadObject，CopyObject和UploadPartCopy中的对source object的读；
// Object的写操作包括：PutObject，PostObject，AppendObject，DeleteObject，
// DeleteMultipleObjects，CompleteMultipartUpload以及CopyObject对新的Object的写。
//
// objectKey 设置权限的object。
// objectAcl 对象权限。可选值PrivateACL(私有读写)、PublicReadACL(公共读私有写)、PublicReadWriteACL(公共读写)。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) SetObjectACL(objectKey string, objectACL ACLType) error {
	options := []Option{ObjectACL(objectACL)}
	params := map[string]interface{}{}
	params["acl"] = nil
	resp, err := bucket.do("PUT", objectKey, params, options, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// GetObjectACL 获取对象的ACL权限。
//
// objectKey 获取权限的object。
//
// GetObjectAclResponse 获取权限操作返回值，error为nil时有效。GetObjectAclResponse.Acl为对象的权限。
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) GetObjectACL(objectKey string) (GetObjectACLResult, error) {
	var out GetObjectACLResult
	params := map[string]interface{}{}
	params["acl"] = nil
	resp, err := bucket.do("GET", objectKey, params, nil, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// PutSymlink 创建符号链接。
//
// 符号链接的目标文件类型不能为符号链接。
// 创建符号链接时: 不检查目标文件是否存在, 不检查目标文件类型是否合法, 不检查目标文件是否有权限访问。
// 以上检查，都推迟到GetObject等需要访问目标文件的API。
// 如果试图添加的文件已经存在，并且有访问权限。新添加的文件将覆盖原来的文件。
// 如果在PutSymlink的时候，携带以x-oss-meta-为前缀的参数，则视为user meta。
//
// symObjectKey 要创建的符号链接文件。
// targetObjectKey 目标文件。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) PutSymlink(symObjectKey string, targetObjectKey string, options ...Option) error {
	options = append(options, symlinkTarget(url.QueryEscape(targetObjectKey)))
	params := map[string]interface{}{}
	params["symlink"] = nil
	resp, err := bucket.do("PUT", symObjectKey, params, options, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK})
}

//
// GetSymlink 获取符号链接的目标文件。
// 如果符号链接不存在返回404。
//
// objectKey 获取目标文件的符号链接object。
//
// error 操作无错误为nil，非nil为错误信息。当error为nil时，返回的string为目标文件，否则该值无效。
//
func (bucket Bucket) GetSymlink(objectKey string) (http.Header, error) {
	params := map[string]interface{}{}
	params["symlink"] = nil
	resp, err := bucket.do("GET", objectKey, params, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	targetObjectKey := resp.Headers.Get(HTTPHeaderOssSymlinkTarget)
	targetObjectKey, err = url.QueryUnescape(targetObjectKey)
	if err != nil {
		return resp.Headers, err
	}
	resp.Headers.Set(HTTPHeaderOssSymlinkTarget, targetObjectKey)
	return resp.Headers, err
}

//
// RestoreObject 恢复处于冷冻状态的归档类型Object进入读就绪状态。
//
// 一个Archive类型的object初始时处于冷冻状态。
//
// 针对处于冷冻状态的object调用restore命令，返回成功。object处于解冻中，服务端执行解冻，在此期间再次调用restore命令，同样成功，且不会延长object可读状态持续时间。
// 待服务端执行完成解冻任务后，object就进入了解冻状态，此时用户可以读取object。
// 解冻状态默认持续1天，对于解冻状态的object调用restore命令，会将object的解冻状态延长一天，最多可以延长到7天，之后object又回到初始时的冷冻状态。
//
// objectKey 需要恢复状态的object名称。
//
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) RestoreObject(objectKey string) error {
	params := map[string]interface{}{}
	params["restore"] = nil
	resp, err := bucket.do("POST", objectKey, params, nil, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusOK, http.StatusAccepted})
}

//
// SignURL 获取签名URL。
//
// objectKey 获取URL的object。
// signURLConfig 获取URL的配置。
//
// 返回URL字符串，error为nil时有效。
// error 操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) SignURL(objectKey string, method HTTPMethod, expiredInSec int64, options ...Option) (string, error) {
	if expiredInSec < 0 {
		return "", fmt.Errorf("invalid expires: %d, expires must bigger than 0", expiredInSec)
	}
	expiration := time.Now().Unix() + expiredInSec

	params, err := getRawParams(options)
	if err != nil {
		return "", err
	}

	headers := make(map[string]string)
	err = handleOptions(headers, options)
	if err != nil {
		return "", err
	}

	return bucket.Client.Conn.signURL(method, bucket.BucketName, objectKey, expiration, params, headers), nil
}

//
// PutObjectWithURL 新建Object，如果Object已存在，覆盖原有Object。
// PutObjectWithURL 不会根据key生成minetype。
//
// signedURL  签名的URL。
// reader     io.Reader读取object的数据。
// options    上传对象时可以指定对象的属性，可用选项有CacheControl、ContentDisposition、ContentEncoding、
// Expires、ServerSideEncryption、ObjectACL、Meta，具体含义请参看
// https://help.aliyun.com/document_detail/oss/api-reference/object/PutObject.html
//
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) PutObjectWithURL(signedURL string, reader io.Reader, options ...Option) error {
	resp, err := bucket.DoPutObjectWithURL(signedURL, reader, options)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return err
}

//
// PutObjectFromFileWithURL 新建Object，内容从本地文件中读取。
// PutObjectFromFileWithURL 不会根据key、filePath生成minetype。
//
// signedURL  签名的URL。
// filePath  本地文件，如 dir/file.txt，上传对象的值为该文件内容。
// options   上传对象时可以指定对象的属性。详见PutObject的options。
//
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) PutObjectFromFileWithURL(signedURL, filePath string, options ...Option) error {
	fd, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	resp, err := bucket.DoPutObjectWithURL(signedURL, fd, options)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return err
}

//
// DoPutObjectWithURL 上传文件。
//
// signedURL  签名的URL。
// reader     io.Reader读取object的数据。
// options  上传选项。
//
// Response 上传请求返回值。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) DoPutObjectWithURL(signedURL string, reader io.Reader, options []Option) (*Response, error) {
	listener := getProgressListener(options)

	params := map[string]interface{}{}
	resp, err := bucket.doURL("PUT", signedURL, params, options, reader, listener)
	if err != nil {
		return nil, err
	}

	if bucket.getConfig().IsEnableCRC {
		err = checkCRC(resp, "DoPutObjectWithURL")
		if err != nil {
			return resp, err
		}
	}

	err = checkRespCode(resp.StatusCode, []int{http.StatusOK})

	return resp, err
}

//
// GetObjectWithURL 下载文件。
//
// signedURL  签名的URL。
// options   对象的属性限制项，可选值有Range、IfModifiedSince、IfUnmodifiedSince、IfMatch、
// IfNoneMatch、AcceptEncoding，详细请参考
// https://help.aliyun.com/document_detail/oss/api-reference/object/GetObject.html
//
// io.ReadCloser  reader，读取数据后需要close。error为nil时有效。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) GetObjectWithURL(signedURL string, options ...Option) (io.ReadCloser, error) {
	result, err := bucket.DoGetObjectWithURL(signedURL, options)
	if err != nil {
		return nil, err
	}
	return result.Response.Body, nil
}

//
// GetObjectToFile 下载文件。
//
// signedURL  签名的URL。
// filePath   下载对象的内容写到该本地文件。
// options    对象的属性限制项。详见GetObject的options。
//
// error  操作无错误时返回error为nil，非nil为错误说明。
//
func (bucket Bucket) GetObjectToFileWithURL(signedURL, filePath string, options ...Option) error {
	tempFilePath := filePath + TempFileSuffix

	// 读取Object内容
	result, err := bucket.DoGetObjectWithURL(signedURL, options)
	if err != nil {
		return err
	}
	defer result.Response.Body.Close()

	// 如果文件不存在则创建，存在则清空
	fd, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, FilePermMode)
	if err != nil {
		return err
	}

	// 存储数据到文件
	_, err = io.Copy(fd, result.Response.Body)
	fd.Close()
	if err != nil {
		return err
	}

	// 比较CRC值
	hasRange, _, _ := isOptionSet(options, HTTPHeaderRange)
	if bucket.getConfig().IsEnableCRC && !hasRange {
		result.Response.ClientCRC = result.ClientCRC.Sum64()
		err = checkCRC(result.Response, "GetObjectToFileWithURL")
		if err != nil {
			os.Remove(tempFilePath)
			return err
		}
	}

	return os.Rename(tempFilePath, filePath)
}

//
// DoGetObjectWithURL 下载文件
//
// signedURL  签名的URL。
// options    对象的属性限制项。详见GetObject的options。
//
// GetObjectResult 下载请求返回值。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) DoGetObjectWithURL(signedURL string, options []Option) (*GetObjectResult, error) {
	params := map[string]interface{}{}
	resp, err := bucket.doURL("GET", signedURL, params, options, nil, nil)
	if err != nil {
		return nil, err
	}

	result := &GetObjectResult{
		Response: resp,
	}

	// crc
	var crcCalc hash.Hash64
	hasRange, _, _ := isOptionSet(options, HTTPHeaderRange)
	if bucket.getConfig().IsEnableCRC && !hasRange {
		crcCalc = crc64.New(crcTable())
		result.ServerCRC = resp.ServerCRC
		result.ClientCRC = crcCalc
	}

	// progress
	listener := getProgressListener(options)

	contentLen, _ := strconv.ParseInt(resp.Headers.Get(HTTPHeaderContentLength), 10, 64)
	resp.Body = ioutil.NopCloser(TeeReader(resp.Body, crcCalc, contentLen, listener, nil))

	return result, nil
}

// Private
func (bucket Bucket) do(method, objectName string, params map[string]interface{}, options []Option,
	data io.Reader, listener ProgressListener) (*Response, error) {
	headers := make(map[string]string)
	err := handleOptions(headers, options)
	if err != nil {
		return nil, err
	}
	return bucket.Client.Conn.Do(method, bucket.BucketName, objectName,
		params, headers, data, 0, listener)
}

func (bucket Bucket) doURL(method HTTPMethod, signedURL string, params map[string]interface{}, options []Option,
	data io.Reader, listener ProgressListener) (*Response, error) {
	headers := make(map[string]string)
	err := handleOptions(headers, options)
	if err != nil {
		return nil, err
	}
	return bucket.Client.Conn.DoURL(method, signedURL, headers, data, 0, listener)
}

func (bucket Bucket) getConfig() *Config {
	return bucket.Client.Config
}

func addContentType(options []Option, keys ...string) []Option {
	typ := TypeByExtension("")
	for _, key := range keys {
		typ = TypeByExtension(key)
		if typ != "" {
			break
		}
	}

	if typ == "" {
		typ = "application/octet-stream"
	}

	opts := []Option{ContentType(typ)}
	opts = append(opts, options...)

	return opts
}
