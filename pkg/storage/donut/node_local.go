package donut

import (
	"errors"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"encoding/json"
	"path/filepath"
)

type localDirectoryNode struct {
	root string
}

func (d localDirectoryNode) CreateBucket(bucket string) error {
	objectPath := path.Join(d.root, bucket)
	return os.MkdirAll(objectPath, 0700)
}

func (d localDirectoryNode) GetBuckets() ([]string, error) {
	return nil, errors.New("Not Implemented")
}

func (d localDirectoryNode) GetWriter(bucket, object string) (Writer, error) {
	objectPath := path.Join(d.root, bucket, object)
	err := os.MkdirAll(objectPath, 0700)
	if err != nil {
		return nil, err
	}
	return newDonutObjectWriter(objectPath)
}

func (d localDirectoryNode) GetReader(bucket, object string) (io.ReadCloser, error) {
	return os.Open(path.Join(d.root, bucket, object, "data"))
}

func (d localDirectoryNode) GetMetadata(bucket, object string) (map[string]string, error) {
	return d.getMetadata(bucket, object, "metadata.json")
}
func (d localDirectoryNode) GetDonutMetadata(bucket, object string) (map[string]string, error) {
	return d.getMetadata(bucket, object, "donutMetadata.json")
}

func (d localDirectoryNode) getMetadata(bucket, object, fileName string) (map[string]string, error) {
	file, err := os.Open(path.Join(d.root, bucket, object, fileName))
	defer file.Close()
	if err != nil {
		return nil, err
	}
	metadata := make(map[string]string)
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	return metadata, nil

}

func (d localDirectoryNode) ListObjects(bucketName string) ([]string, error) {
	prefix := path.Join(d.root, bucketName)
	var objects []string
	if err := filepath.Walk(prefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, "data") {
			object := strings.TrimPrefix(path, prefix+"/")
			object = strings.TrimSuffix(object, "/data")
			objects = append(objects, object)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(objects)
	return objects, nil
}
