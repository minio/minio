package minioapi

import (
	"encoding/xml"
)

type ListResponse struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string
	Marker      string
	MaxKeys     int32
	IsTruncated bool
	Contents    []Content `xml:"Contents",innerxml`
}

type Content struct {
	Key          string
	LastModified string
	ETag         string
	Size         uint64
	StorageClass string
	Owner        Owner
}

type Owner struct {
	ID          string
	DisplayName string
}
