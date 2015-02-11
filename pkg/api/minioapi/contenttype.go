package minioapi

import (
	"net/http"
)

type contentType int

const (
	xmlType contentType = iota
	jsonType
)

var typeToString = map[contentType]string{
	xmlType:  "application/xml",
	jsonType: "application/json",
}
var acceptToType = map[string]contentType{
	"application/xml":  xmlType,
	"application/json": jsonType,
}

func getContentType(req *http.Request) contentType {
	if accept := req.Header.Get("Accept"); accept != "" {
		return acceptToType[accept]
	}
	return xmlType
}

func getContentString(content contentType) string {
	return typeToString[content]
}
