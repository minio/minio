package donut

import (
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
)

type donutDriver struct {
	buckets map[string]Bucket
	nodes   map[string]Node
}

// NewDonutDriver - instantiate new donut driver
func NewDonutDriver(root string) Donut {
	nodes := make(map[string]Node)
	nodes["localhost"] = localDirectoryNode{root: root}
	driver := donutDriver{
		buckets: make(map[string]Bucket),
		nodes:   nodes,
	}
	return driver
}

func (driver donutDriver) CreateBucket(bucketName string) error {
	if _, ok := driver.buckets[bucketName]; ok == false {
		bucketName = strings.TrimSpace(bucketName)
		if bucketName == "" {
			return errors.New("Cannot create bucket with no name")
		}
		// assign nodes
		// TODO assign other nodes
		nodes := make([]string, 16)
		for i := 0; i < 16; i++ {
			nodes[i] = "localhost"
		}
		bucket := bucketDriver{
			nodes: nodes,
		}
		driver.buckets[bucketName] = bucket
		return nil
	}
	return errors.New("Bucket exists")
}

func (driver donutDriver) ListBuckets() ([]string, error) {
	var buckets []string
	for bucket := range driver.buckets {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	return buckets, nil
}

func (driver donutDriver) GetObjectWriter(bucketName, objectName string) (ObjectWriter, error) {
	if bucket, ok := driver.buckets[bucketName]; ok == true {
		writers := make([]Writer, 16)
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, err
		}
		for i, nodeID := range nodes {
			if node, ok := driver.nodes[nodeID]; ok == true {
				writer, _ := node.GetWriter(bucketName+":0:"+strconv.Itoa(i), objectName)
				writers[i] = writer
			}
		}
		return newErasureWriter(writers), nil
	}
	return nil, errors.New("Bucket not found")
}

func (driver donutDriver) GetObject(bucketName, objectName string) (io.ReadCloser, error) {
	r, w := io.Pipe()
	if bucket, ok := driver.buckets[bucketName]; ok == true {
		readers := make([]io.ReadCloser, 16)
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, err
		}
		var metadata map[string]string
		for i, nodeID := range nodes {
			if node, ok := driver.nodes[nodeID]; ok == true {
				bucketID := bucketName + ":0:" + strconv.Itoa(i)
				reader, err := node.GetReader(bucketID, objectName)
				if err != nil {
					return nil, err
				}
				readers[i] = reader
				if metadata == nil {
					metadata, err = node.GetDonutMetadata(bucketID, objectName)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		go erasureReader(readers, metadata, w)
		return r, nil
	}
	return nil, errors.New("Bucket not found")
}

// GetObjectMetadata returns metadata for a given object in a bucket
func (driver donutDriver) GetObjectMetadata(bucketName, object string) (map[string]string, error) {
	if bucket, ok := driver.buckets[bucketName]; ok {
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, err
		}
		if node, ok := driver.nodes[nodes[0]]; ok {
			return node.GetMetadata(bucketName+":0:0", object)
		}
		return nil, errors.New("Cannot connect to node: " + nodes[0])
	}
	return nil, errors.New("Bucket not found")
}

func (driver donutDriver) ListObjects(bucketName string) ([]string, error) {
	if bucket, ok := driver.buckets[bucketName]; ok {
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, err
		}
		if node, ok := driver.nodes[nodes[0]]; ok {
			return node.ListObjects(bucketName + ":0:0")
		}
	}
	return nil, errors.New("Bucket not found")
}
