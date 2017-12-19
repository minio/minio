package oss

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// ServiceError contains fields of the error response from Oss Service REST API.
type ServiceError struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`      // OSS返回给用户的错误码
	Message    string   `xml:"Message"`   // OSS给出的详细错误信息
	RequestID  string   `xml:"RequestId"` // 用于唯一标识该次请求的UUID
	HostID     string   `xml:"HostId"`    // 用于标识访问的OSS集群
	RawMessage string   // OSS返回的原始消息内容
	StatusCode int      // HTTP状态码
}

// Implement interface error
func (e ServiceError) Error() string {
	return fmt.Sprintf("oss: service returned error: StatusCode=%d, ErrorCode=%s, ErrorMessage=%s, RequestId=%s",
		e.StatusCode, e.Code, e.Message, e.RequestID)
}

// UnexpectedStatusCodeError is returned when a storage service responds with neither an error
// nor with an HTTP status code indicating success.
type UnexpectedStatusCodeError struct {
	allowed []int // 预期OSS返回HTTP状态码
	got     int   // OSS实际返回HTTP状态码
}

// Implement interface error
func (e UnexpectedStatusCodeError) Error() string {
	s := func(i int) string { return fmt.Sprintf("%d %s", i, http.StatusText(i)) }

	got := s(e.got)
	expected := []string{}
	for _, v := range e.allowed {
		expected = append(expected, s(v))
	}
	return fmt.Sprintf("oss: status code from service response is %s; was expecting %s",
		got, strings.Join(expected, " or "))
}

// Got is the actual status code returned by oss.
func (e UnexpectedStatusCodeError) Got() int {
	return e.got
}

// checkRespCode returns UnexpectedStatusError if the given response code is not
// one of the allowed status codes; otherwise nil.
func checkRespCode(respCode int, allowed []int) error {
	for _, v := range allowed {
		if respCode == v {
			return nil
		}
	}
	return UnexpectedStatusCodeError{allowed, respCode}
}

// CRCCheckError is returned when crc check is inconsistent between client and server
type CRCCheckError struct {
	clientCRC uint64 // 客户端计算的CRC64值
	serverCRC uint64 // 服务端计算的CRC64值
	operation string // 上传操作，如PutObject/AppendObject/UploadPart等
	requestID string // 本次操作的RequestID
}

// Implement interface error
func (e CRCCheckError) Error() string {
	return fmt.Sprintf("oss: the crc of %s is inconsistent, client %d but server %d; request id is %s",
		e.operation, e.clientCRC, e.serverCRC, e.requestID)
}

func checkDownloadCRC(clientCRC, serverCRC uint64) error {
	if clientCRC == serverCRC {
		return nil
	}
	return CRCCheckError{clientCRC, serverCRC, "DownloadFile", ""}
}

func checkCRC(resp *Response, operation string) error {
	if resp.Headers.Get(HTTPHeaderOssCRC64) == "" || resp.ClientCRC == resp.ServerCRC {
		return nil
	}
	return CRCCheckError{resp.ClientCRC, resp.ServerCRC, operation, resp.Headers.Get(HTTPHeaderOssRequestID)}
}
