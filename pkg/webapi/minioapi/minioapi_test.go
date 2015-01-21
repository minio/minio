package minioapi

import (
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"testing"
)

func TestMinioApi(t *testing.T) {
	owner := Owner{
		ID:          "MyID",
		DisplayName: "MyDisplayName",
	}
	contents := []Content{
		Content{
			Key:          "one",
			LastModified: "two",
			ETag:         "\"ETag\"",
			Size:         1,
			StorageClass: "three",
			Owner:        owner,
		},
		Content{
			Key:          "four",
			LastModified: "five",
			ETag:         "\"ETag\"",
			Size:         1,
			StorageClass: "six",
			Owner:        owner,
		},
	}
	data := &ListResponse{
		Name:     "name",
		Contents: contents,
	}

	xmlEncoder := xml.NewEncoder(os.Stdout)
	if err := xmlEncoder.Encode(data); err != nil {
		log.Println(err)
	} else {
		fmt.Println("")
	}
}
