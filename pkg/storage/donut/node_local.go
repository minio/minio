package donut

import (
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"encoding/json"
	"github.com/minio-io/iodine"
	"io/ioutil"
	"path/filepath"
)

type localDirectoryNode struct {
	root string
}

func (d localDirectoryNode) CreateBucket(bucket string) error {
	objectPath := path.Join(d.root, bucket)
	return iodine.New(os.MkdirAll(objectPath, 0700), map[string]string{"bucket": bucket})
}

func (d localDirectoryNode) GetBuckets() ([]string, error) {
	files, err := ioutil.ReadDir(d.root)
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	var results []string
	for _, file := range files {
		if file.IsDir() {
			results = append(results, file.Name())
		}
	}
	return results, nil
}

func (d localDirectoryNode) GetWriter(bucket, object string) (Writer, error) {
	errParams := map[string]string{"bucket": bucket, "object": object}
	objectPath := path.Join(d.root, bucket, object)
	err := os.MkdirAll(objectPath, 0700)
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	writer, err := newDonutObjectWriter(objectPath)
	return writer, iodine.New(err, errParams)
}

func (d localDirectoryNode) GetReader(bucket, object string) (io.ReadCloser, error) {
	reader, err := os.Open(path.Join(d.root, bucket, object, "data"))
	return reader, iodine.New(err, map[string]string{"bucket": bucket, "object": object})
}

func (d localDirectoryNode) GetMetadata(bucket, object string) (map[string]string, error) {
	m, err := d.getMetadata(bucket, object, "metadata.json")
	return m, iodine.New(err, map[string]string{"bucket": bucket, "object": object})
}
func (d localDirectoryNode) GetDonutMetadata(bucket, object string) (map[string]string, error) {
	m, err := d.getMetadata(bucket, object, "donutMetadata.json")
	return m, iodine.New(err, map[string]string{"bucket": bucket, "object": object})
}

func (d localDirectoryNode) getMetadata(bucket, object, fileName string) (map[string]string, error) {
	errParams := map[string]string{"bucket": bucket, "object": object, "file": fileName}
	file, err := os.Open(path.Join(d.root, bucket, object, fileName))
	defer file.Close()
	if err != nil {
		return nil, iodine.New(err, errParams)
	}
	metadata := make(map[string]string)
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, iodine.New(err, errParams)
	}
	return metadata, nil

}

func (d localDirectoryNode) ListObjects(bucketName string) ([]string, error) {
	errParams := map[string]string{"bucket": bucketName}
	prefix := path.Join(d.root, bucketName)
	var objects []string
	if err := filepath.Walk(prefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return iodine.New(err, errParams)
		}
		if !info.IsDir() && strings.HasSuffix(path, "data") {
			object := strings.TrimPrefix(path, prefix+"/")
			object = strings.TrimSuffix(object, "/data")
			objects = append(objects, object)
		}
		return nil
	}); err != nil {
		return nil, iodine.New(err, errParams)
	}
	sort.Strings(objects)
	return objects, nil
}
