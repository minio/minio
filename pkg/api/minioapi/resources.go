package minioapi

import (
	"net/url"
	"strconv"
)

type bucketResources struct {
	prefix    string
	marker    string
	maxkeys   int
	policy    bool
	delimiter string
	//	uploads   bool - TODO implemented with multipart support
}

func getBucketResources(values url.Values) (v bucketResources) {
	for key, value := range values {
		switch true {
		case key == "prefix":
			v.prefix = value[0]
		case key == "marker":
			v.marker = value[0]
		case key == "maxkeys":
			v.maxkeys, _ = strconv.Atoi(value[0])
		case key == "policy":
			v.policy = true
		case key == "delimiter":
			v.delimiter = value[0]
		}
	}
	return
}
