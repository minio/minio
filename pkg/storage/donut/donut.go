package donut

import (
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/minio-io/iodine"
)

type donut struct {
	buckets map[string]Bucket
	nodes   map[string]Node
}

// NewDonut - instantiate new donut driver
func NewDonut(root string) (Donut, error) {
	nodes := make(map[string]Node)
	nodes["localhost"] = &localDirectoryNode{root: root}
	driver := &donut{
		buckets: make(map[string]Bucket),
		nodes:   nodes,
	}
	for nodeID, node := range nodes {
		bucketIDs, err := node.GetBuckets()
		if err != nil {
			return nil, iodine.New(err, map[string]string{"root": root})
		}
		for _, bucketID := range bucketIDs {
			tokens := strings.Split(bucketID, ":")
			if _, ok := driver.buckets[tokens[0]]; !ok {
				bucket := donutBucket{
					nodes: make([]string, 16),
				}
				// TODO catch errors
				driver.buckets[tokens[0]] = bucket
			}
			if err = driver.buckets[tokens[0]].AddNode(nodeID, bucketID); err != nil {
				return nil, iodine.New(err, map[string]string{"root": root})
			}
		}
	}
	return driver, nil
}

// CreateBucket - create a new bucket
func (d donut) CreateBucket(bucketName string) error {
	if _, ok := d.buckets[bucketName]; ok == false {
		bucketName = strings.TrimSpace(bucketName)
		if bucketName == "" {
			return iodine.New(errors.New("Cannot create bucket with no name"), map[string]string{"bucket": bucketName})
		}
		// assign nodes
		// TODO assign other nodes
		nodes := make([]string, 16)
		for i := 0; i < 16; i++ {
			nodes[i] = "localhost"
			if node, ok := d.nodes["localhost"]; ok {
				err := node.CreateBucket(bucketName + ":0:" + strconv.Itoa(i))
				if err != nil {
					return iodine.New(err, map[string]string{"node": nodes[i], "bucket": bucketName})
				}
			}
		}
		bucket := donutBucket{
			nodes: nodes,
		}
		d.buckets[bucketName] = bucket
		return nil
	}
	return iodine.New(errors.New("Bucket exists"), map[string]string{"bucket": bucketName})
}

// ListBuckets - list all buckets
func (d donut) ListBuckets() ([]string, error) {
	var buckets []string
	for bucket := range d.buckets {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	return buckets, nil
}

// GetObjectWriter - get a new writer interface for a new object
func (d donut) GetObjectWriter(bucketName, objectName string) (ObjectWriter, error) {
	if bucket, ok := d.buckets[bucketName]; ok == true {
		writers := make([]Writer, 16)
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, iodine.New(err, map[string]string{"bucket": bucketName, "object": objectName})
		}
		for i, nodeID := range nodes {
			if node, ok := d.nodes[nodeID]; ok == true {
				bucketID := bucketName + ":0:" + strconv.Itoa(i)
				writer, err := node.GetWriter(bucketID, objectName)
				if err != nil {
					for _, writerToClose := range writers {
						if writerToClose != nil {
							writerToClose.CloseWithError(iodine.New(err, nil))
						}
					}
					return nil, iodine.New(err, map[string]string{"bucketid": bucketID})
				}
				writers[i] = writer
			}
		}
		return newErasureWriter(writers), nil
	}
	return nil, iodine.New(errors.New("Bucket not found"), map[string]string{"bucket": bucketName})
}

// GetObjectReader - get a new reader interface for a new object
func (d donut) GetObjectReader(bucketName, objectName string) (io.ReadCloser, error) {
	errParams := map[string]string{"bucket": bucketName, "object": objectName}
	r, w := io.Pipe()
	if bucket, ok := d.buckets[bucketName]; ok == true {
		readers := make([]io.ReadCloser, 16)
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, iodine.New(err, errParams)
		}
		var metadata map[string]string
		for i, nodeID := range nodes {
			if node, ok := d.nodes[nodeID]; ok == true {
				bucketID := bucketName + ":0:" + strconv.Itoa(i)
				reader, err := node.GetReader(bucketID, objectName)
				if err != nil {
					errParams["node"] = nodeID
					return nil, iodine.New(err, errParams)
				}
				readers[i] = reader
				if metadata == nil {
					metadata, err = node.GetDonutMetadata(bucketID, objectName)
					if err != nil {
						errParams["node"] = nodeID
						return nil, iodine.New(err, errParams)
					}
				}
			}
		}
		go erasureReader(readers, metadata, w)
		return r, nil
	}
	return nil, iodine.New(errors.New("Bucket not found"), errParams)
}

// GetObjectMetadata returns metadata for a given object in a bucket
func (d donut) GetObjectMetadata(bucketName, object string) (map[string]string, error) {
	errParams := map[string]string{"bucket": bucketName, "object": object}
	if bucket, ok := d.buckets[bucketName]; ok {
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, iodine.New(err, errParams)
		}
		if node, ok := d.nodes[nodes[0]]; ok {
			bucketID := bucketName + ":0:0"
			metadata, err := node.GetMetadata(bucketID, object)
			if err != nil {
				errParams["bucketID"] = bucketID
				return nil, iodine.New(err, errParams)
			}
			donutMetadata, err := node.GetDonutMetadata(bucketID, object)
			if err != nil {
				errParams["bucketID"] = bucketID
				return nil, iodine.New(err, errParams)
			}
			metadata["sys.created"] = donutMetadata["created"]
			metadata["sys.md5"] = donutMetadata["md5"]
			metadata["sys.size"] = donutMetadata["size"]
			return metadata, nil
		}
		errParams["node"] = nodes[0]
		return nil, iodine.New(errors.New("Cannot connect to node: "+nodes[0]), errParams)
	}
	return nil, errors.New("Bucket not found")
}

// ListObjects - list all the available objects in a bucket
func (d donut) ListObjects(bucketName string) ([]string, error) {
	errParams := map[string]string{"bucket": bucketName}
	if bucket, ok := d.buckets[bucketName]; ok {
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, iodine.New(err, errParams)
		}
		if node, ok := d.nodes[nodes[0]]; ok {
			bucketID := bucketName + ":0:0"
			objects, err := node.ListObjects(bucketID)
			errParams["bucketID"] = bucketID
			return objects, iodine.New(err, errParams)
		}
	}
	return nil, iodine.New(errors.New("Bucket not found"), errParams)
}
