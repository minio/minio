package config

import (
	"encoding/json"
	"io/fs"
	"time"
)

// ObjectInfo - object information returned by Panasas config agent
type ObjectInfo struct {
	ID         string            `json:"id"`
	Metadata   map[string]string `json:"metadata"`
	Namespace  NamespaceInfo     `json:"namespace"`
	ChangedAt  time.Time         `json:"mod-time"`
	ByteLength int64             `json:"size"`
}

// Name - return name of the object
func (poi *ObjectInfo) Name() string {
	return poi.ID
}

// Size - return object's byte length
func (poi *ObjectInfo) Size() int64 {
	return poi.ByteLength
}

// Mode - return the file mode bits for the object. The permissions are not
// supported so the value will be 0.
func (poi *ObjectInfo) Mode() fs.FileMode {
	return fs.FileMode(0)
}

// ModTime - returns object modification time
func (poi *ObjectInfo) ModTime() time.Time {
	return poi.ChangedAt
}

// IsDir - returns boolean to indicate if the object is a directory
func (poi *ObjectInfo) IsDir() bool {
	return false
}

// Sys - return pointer to some internal data
func (poi *ObjectInfo) Sys() any {
	return nil
}

func parseObjectInfo(JSONData string) (*ObjectInfo, error) {
	var oi ObjectInfo

	if err := json.Unmarshal([]byte(JSONData), &oi); err != nil {
		return nil, err
	}
	return &oi, nil
}
