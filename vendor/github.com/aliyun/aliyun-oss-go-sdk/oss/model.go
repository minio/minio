package oss

import (
	"hash"
	"io"
	"net/http"
)

// Response Http response from oss
type Response struct {
	StatusCode int
	Headers    http.Header
	Body       io.ReadCloser
	ClientCRC  uint64
	ServerCRC  uint64
}

// PutObjectRequest The request of DoPutObject
type PutObjectRequest struct {
	ObjectKey string
	Reader    io.Reader
}

// GetObjectRequest The request of DoGetObject
type GetObjectRequest struct {
	ObjectKey string
}

// GetObjectResult The result of DoGetObject
type GetObjectResult struct {
	Response  *Response
	ClientCRC hash.Hash64
	ServerCRC uint64
}

// AppendObjectRequest  The requtest of DoAppendObject
type AppendObjectRequest struct {
	ObjectKey string
	Reader    io.Reader
	Position  int64
}

// AppendObjectResult The result of DoAppendObject
type AppendObjectResult struct {
	NextPosition int64
	CRC          uint64
}

// UploadPartRequest The request of DoUploadPart
type UploadPartRequest struct {
	InitResult *InitiateMultipartUploadResult
	Reader     io.Reader
	PartSize   int64
	PartNumber int
}

// UploadPartResult The result of DoUploadPart
type UploadPartResult struct {
	Part UploadPart
}
