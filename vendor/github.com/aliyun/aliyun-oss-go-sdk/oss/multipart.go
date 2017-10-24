package oss

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
)

//
// InitiateMultipartUpload 初始化分片上传任务。
//
// objectKey  Object名称。
// options    上传时可以指定Object的属性，可选属性有CacheControl、ContentDisposition、ContentEncoding、Expires、
// ServerSideEncryption、Meta，具体含义请参考
// https://help.aliyun.com/document_detail/oss/api-reference/multipart-upload/InitiateMultipartUpload.html
//
// InitiateMultipartUploadResult 初始化后操作成功的返回值，用于后面的UploadPartFromFile、UploadPartCopy等操作。error为nil时有效。
// error  操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) InitiateMultipartUpload(objectKey string, options ...Option) (InitiateMultipartUploadResult, error) {
	var imur InitiateMultipartUploadResult
	opts := addContentType(options, objectKey)
	params := map[string]interface{}{}
	params["uploads"] = nil
	resp, err := bucket.do("POST", objectKey, params, opts, nil, nil)
	if err != nil {
		return imur, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &imur)
	return imur, err
}

//
// UploadPart 上传分片。
//
// 初始化一个Multipart Upload之后，可以根据指定的Object名和Upload ID来分片（Part）上传数据。
// 每一个上传的Part都有一个标识它的号码（part number，范围是1~10000）。对于同一个Upload ID，
// 该号码不但唯一标识这一片数据，也标识了这片数据在整个文件内的相对位置。如果您用同一个part号码，上传了新的数据，
// 那么OSS上已有的这个号码的Part数据将被覆盖。除了最后一片Part以外，其他的part最小为100KB；
// 最后一片Part没有大小限制。
//
// imur        InitiateMultipartUpload成功后的返回值。
// reader      io.Reader 需要分片上传的reader。
// size        本次上传片Part的大小。
// partNumber  本次上传片(Part)的编号，范围是1~10000。如果超出范围，OSS将返回InvalidArgument错误。
//
// UploadPart 上传成功的返回值，两个成员PartNumber、ETag。PartNumber片编号，即传入参数partNumber；
// ETag及上传数据的MD5。error为nil时有效。
// error 操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) UploadPart(imur InitiateMultipartUploadResult, reader io.Reader,
	partSize int64, partNumber int, options ...Option) (UploadPart, error) {
	request := &UploadPartRequest{
		InitResult: &imur,
		Reader:     reader,
		PartSize:   partSize,
		PartNumber: partNumber,
	}

	result, err := bucket.DoUploadPart(request, options)

	return result.Part, err
}

//
// UploadPartFromFile 上传分片。
//
// imur           InitiateMultipartUpload成功后的返回值。
// filePath       需要分片上传的本地文件。
// startPosition  本次上传文件片的起始位置。
// partSize       本次上传文件片的大小。
// partNumber     本次上传文件片的编号，范围是1~10000。
//
// UploadPart 上传成功的返回值，两个成员PartNumber、ETag。PartNumber片编号，传入参数partNumber；
// ETag上传数据的MD5。error为nil时有效。
// error 操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) UploadPartFromFile(imur InitiateMultipartUploadResult, filePath string,
	startPosition, partSize int64, partNumber int, options ...Option) (UploadPart, error) {
	var part = UploadPart{}
	fd, err := os.Open(filePath)
	if err != nil {
		return part, err
	}
	defer fd.Close()
	fd.Seek(startPosition, os.SEEK_SET)

	request := &UploadPartRequest{
		InitResult: &imur,
		Reader:     fd,
		PartSize:   partSize,
		PartNumber: partNumber,
	}

	result, err := bucket.DoUploadPart(request, options)

	return result.Part, err
}

//
// DoUploadPart 上传分片。
//
// request 上传分片请求。
//
// UploadPartResult 上传分片请求返回值。
// error  操作无错误为nil，非nil为错误信息。
//
func (bucket Bucket) DoUploadPart(request *UploadPartRequest, options []Option) (*UploadPartResult, error) {
	listener := getProgressListener(options)
	opts := []Option{ContentLength(request.PartSize)}
	params := map[string]interface{}{}
	params["partNumber"] = strconv.Itoa(request.PartNumber)
	params["uploadId"] = request.InitResult.UploadID
	resp, err := bucket.do("PUT", request.InitResult.Key, params, opts,
		&io.LimitedReader{R: request.Reader, N: request.PartSize}, listener)
	if err != nil {
		return &UploadPartResult{}, err
	}
	defer resp.Body.Close()

	part := UploadPart{
		ETag:       resp.Headers.Get(HTTPHeaderEtag),
		PartNumber: request.PartNumber,
	}

	if bucket.getConfig().IsEnableCRC {
		err = checkCRC(resp, "DoUploadPart")
		if err != nil {
			return &UploadPartResult{part}, err
		}
	}

	return &UploadPartResult{part}, nil
}

//
// UploadPartCopy 拷贝分片。
//
// imur           InitiateMultipartUpload成功后的返回值。
// copySrc        源Object名称。
// startPosition  本次拷贝片(Part)在源Object的起始位置。
// partSize       本次拷贝片的大小。
// partNumber     本次拷贝片的编号，范围是1~10000。如果超出范围，OSS将返回InvalidArgument错误。
// options        copy时源Object的限制条件，满足限制条件时copy，不满足时返回错误。可选条件有CopySourceIfMatch、
// CopySourceIfNoneMatch、CopySourceIfModifiedSince  CopySourceIfUnmodifiedSince，具体含义请参看
// https://help.aliyun.com/document_detail/oss/api-reference/multipart-upload/UploadPartCopy.html
//
// UploadPart 上传成功的返回值，两个成员PartNumber、ETag。PartNumber片(Part)编号，即传入参数partNumber；
// ETag及上传数据的MD5。error为nil时有效。
// error 操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) UploadPartCopy(imur InitiateMultipartUploadResult, srcBucketName, srcObjectKey string,
	startPosition, partSize int64, partNumber int, options ...Option) (UploadPart, error) {
	var out UploadPartCopyResult
	var part UploadPart

	opts := []Option{CopySource(srcBucketName, srcObjectKey),
		CopySourceRange(startPosition, partSize)}
	opts = append(opts, options...)
	params := map[string]interface{}{}
	params["partNumber"] = strconv.Itoa(partNumber)
	params["uploadId"] = imur.UploadID
	resp, err := bucket.do("PUT", imur.Key, params, opts, nil, nil)
	if err != nil {
		return part, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	if err != nil {
		return part, err
	}
	part.ETag = out.ETag
	part.PartNumber = partNumber

	return part, nil
}

//
// CompleteMultipartUpload 提交分片上传任务。
//
// imur   InitiateMultipartUpload的返回值。
// parts  UploadPart/UploadPartFromFile/UploadPartCopy返回值组成的数组。
//
// CompleteMultipartUploadResponse  操作成功后的返回值。error为nil时有效。
// error  操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) CompleteMultipartUpload(imur InitiateMultipartUploadResult,
	parts []UploadPart) (CompleteMultipartUploadResult, error) {
	var out CompleteMultipartUploadResult

	sort.Sort(uploadParts(parts))
	cxml := completeMultipartUploadXML{}
	cxml.Part = parts
	bs, err := xml.Marshal(cxml)
	if err != nil {
		return out, err
	}
	buffer := new(bytes.Buffer)
	buffer.Write(bs)

	params := map[string]interface{}{}
	params["uploadId"] = imur.UploadID
	resp, err := bucket.do("POST", imur.Key, params, nil, buffer, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// AbortMultipartUpload 取消分片上传任务。
//
// imur  InitiateMultipartUpload的返回值。
//
// error  操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) AbortMultipartUpload(imur InitiateMultipartUploadResult) error {
	params := map[string]interface{}{}
	params["uploadId"] = imur.UploadID
	resp, err := bucket.do("DELETE", imur.Key, params, nil, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkRespCode(resp.StatusCode, []int{http.StatusNoContent})
}

//
// ListUploadedParts 列出指定上传任务已经上传的分片。
//
// imur  InitiateMultipartUpload的返回值。
//
// ListUploadedPartsResponse  操作成功后的返回值，成员UploadedParts已经上传/拷贝的片。error为nil时该返回值有效。
// error  操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) ListUploadedParts(imur InitiateMultipartUploadResult) (ListUploadedPartsResult, error) {
	var out ListUploadedPartsResult
	params := map[string]interface{}{}
	params["uploadId"] = imur.UploadID
	resp, err := bucket.do("GET", imur.Key, params, nil, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	return out, err
}

//
// ListMultipartUploads 列出所有未上传完整的multipart任务列表。
//
// options  ListObject的筛选行为。Prefix返回object的前缀，KeyMarker返回object的起始位置，MaxUploads最大数目默认1000，
// Delimiter用于对Object名字进行分组的字符，所有名字包含指定的前缀且第一次出现delimiter字符之间的object。
//
// ListMultipartUploadResponse  操作成功后的返回值，error为nil时该返回值有效。
// error  操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) ListMultipartUploads(options ...Option) (ListMultipartUploadResult, error) {
	var out ListMultipartUploadResult

	options = append(options, EncodingType("url"))
	params, err := getRawParams(options)
	if err != nil {
		return out, err
	}
	params["uploads"] = nil

	resp, err := bucket.do("GET", "", params, nil, nil, nil)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	err = xmlUnmarshal(resp.Body, &out)
	if err != nil {
		return out, err
	}
	err = decodeListMultipartUploadResult(&out)
	return out, err
}
