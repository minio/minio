package donut

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio-io/minio/pkg/encoding/erasure"
	"github.com/minio-io/minio/pkg/utils/split"
	"path/filepath"
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

func erasureReader(readers []io.ReadCloser, donutMetadata map[string]string, writer *io.PipeWriter) {
	totalChunks, _ := strconv.Atoi(donutMetadata["chunkCount"])
	totalLeft, _ := strconv.Atoi(donutMetadata["totalLength"])
	blockSize, _ := strconv.Atoi(donutMetadata["blockSize"])
	params, _ := erasure.ParseEncoderParams(8, 8, erasure.Cauchy)
	encoder := erasure.NewEncoder(params)
	for _, reader := range readers {
		defer reader.Close()
	}
	for i := 0; i < totalChunks; i++ {
		encodedBytes := make([][]byte, 16)
		for i, reader := range readers {
			var bytesBuffer bytes.Buffer
			io.Copy(&bytesBuffer, reader)
			encodedBytes[i] = bytesBuffer.Bytes()
		}
		curBlockSize := totalLeft
		if blockSize < totalLeft {
			curBlockSize = blockSize
		}
		decodedData, err := encoder.Decode(encodedBytes, curBlockSize)
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		io.Copy(writer, bytes.NewBuffer(decodedData))
		totalLeft = totalLeft - blockSize
	}
	writer.Close()
}

// erasure writer

type erasureWriter struct {
	writers       []Writer
	metadata      map[string]string
	donutMetadata map[string]string // not exposed
	erasureWriter *io.PipeWriter
	isClosed      <-chan bool
}

func newErasureWriter(writers []Writer) ObjectWriter {
	r, w := io.Pipe()
	isClosed := make(chan bool)
	writer := erasureWriter{
		writers:       writers,
		metadata:      make(map[string]string),
		erasureWriter: w,
		isClosed:      isClosed,
	}
	go erasureGoroutine(r, writer, isClosed)
	return writer
}

func erasureGoroutine(r *io.PipeReader, eWriter erasureWriter, isClosed chan<- bool) {
	chunks := split.Stream(r, 10*1024*1024)
	params, _ := erasure.ParseEncoderParams(8, 8, erasure.Cauchy)
	encoder := erasure.NewEncoder(params)
	chunkCount := 0
	totalLength := 0
	for chunk := range chunks {
		if chunk.Err == nil {
			totalLength = totalLength + len(chunk.Data)
			encodedBlocks, _ := encoder.Encode(chunk.Data)
			for blockIndex, block := range encodedBlocks {
				io.Copy(eWriter.writers[blockIndex], bytes.NewBuffer(block))
			}
		}
		chunkCount = chunkCount + 1
	}
	metadata := make(map[string]string)
	metadata["blockSize"] = strconv.Itoa(10 * 1024 * 1024)
	metadata["chunkCount"] = strconv.Itoa(chunkCount)
	metadata["created"] = time.Now().Format(time.RFC3339Nano)
	metadata["erasureK"] = "8"
	metadata["erasureM"] = "8"
	metadata["erasureTechnique"] = "Cauchy"
	metadata["totalLength"] = strconv.Itoa(totalLength)
	for _, nodeWriter := range eWriter.writers {
		if nodeWriter != nil {
			nodeWriter.SetMetadata(eWriter.metadata)
			nodeWriter.SetDonutMetadata(metadata)
			nodeWriter.Close()
		}
	}
	isClosed <- true
}

func (d erasureWriter) Write(data []byte) (int, error) {
	io.Copy(d.erasureWriter, bytes.NewBuffer(data))
	return len(data), nil
}

func (d erasureWriter) Close() error {
	d.erasureWriter.Close()
	<-d.isClosed
	return nil
}

func (d erasureWriter) CloseWithError(err error) error {
	for _, writer := range d.writers {
		if writer != nil {
			writer.CloseWithError(err)
		}
	}
	return nil
}

func (d erasureWriter) SetMetadata(metadata map[string]string) error {
	for k := range d.metadata {
		delete(d.metadata, k)
	}
	for k, v := range metadata {
		d.metadata[k] = v
	}
	return nil
}

func (d erasureWriter) GetMetadata() (map[string]string, error) {
	metadata := make(map[string]string)
	for k, v := range d.metadata {
		metadata[k] = v
	}
	return metadata, nil
}

type localDirectoryNode struct {
	root string
}

func (node localDirectoryNode) GetBuckets() ([]string, error) {
	return nil, errors.New("Not Implemented")
}

func (node localDirectoryNode) GetWriter(bucket, object string) (Writer, error) {
	objectPath := path.Join(node.root, bucket, object)
	err := os.MkdirAll(objectPath, 0700)
	if err != nil {
		return nil, err
	}
	return newDonutFileWriter(objectPath)
}

func (node localDirectoryNode) GetReader(bucket, object string) (io.ReadCloser, error) {
	return os.Open(path.Join(node.root, bucket, object, "data"))
}

func (node localDirectoryNode) GetMetadata(bucket, object string) (map[string]string, error) {
	return node.getMetadata(bucket, object, "metadata.json")
}
func (node localDirectoryNode) GetDonutMetadata(bucket, object string) (map[string]string, error) {
	return node.getMetadata(bucket, object, "donutMetadata.json")
}

func (node localDirectoryNode) getMetadata(bucket, object, fileName string) (map[string]string, error) {
	file, err := os.Open(path.Join(node.root, bucket, object, fileName))
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

func (node localDirectoryNode) ListObjects(bucketName string) ([]string, error) {
	prefix := path.Join(node.root, bucketName)
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

func newDonutFileWriter(objectDir string) (Writer, error) {
	dataFile, err := os.OpenFile(path.Join(objectDir, "data"), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return donutFileWriter{
		root:          objectDir,
		file:          dataFile,
		metadata:      make(map[string]string),
		donutMetadata: make(map[string]string),
	}, nil
}

type donutFileWriter struct {
	root          string
	file          *os.File
	metadata      map[string]string
	donutMetadata map[string]string
	err           error
}

func (d donutFileWriter) Write(data []byte) (int, error) {
	return d.file.Write(data)
}

func (d donutFileWriter) Close() error {
	if d.err != nil {
		return d.err
	}
	metadata, _ := json.Marshal(d.metadata)
	ioutil.WriteFile(path.Join(d.root, "metadata.json"), metadata, 0600)
	donutMetadata, _ := json.Marshal(d.donutMetadata)
	ioutil.WriteFile(path.Join(d.root, "donutMetadata.json"), donutMetadata, 0600)

	return d.file.Close()
}

func (d donutFileWriter) CloseWithError(err error) error {
	if d.err != nil {
		d.err = err
	}
	return d.Close()
}

func (d donutFileWriter) SetMetadata(metadata map[string]string) error {
	for k := range d.metadata {
		delete(d.metadata, k)
	}
	for k, v := range metadata {
		d.metadata[k] = v
	}
	return nil
}

func (d donutFileWriter) GetMetadata() (map[string]string, error) {
	metadata := make(map[string]string)
	for k, v := range d.metadata {
		metadata[k] = v
	}
	return metadata, nil
}

func (d donutFileWriter) SetDonutMetadata(metadata map[string]string) error {
	for k := range d.donutMetadata {
		delete(d.donutMetadata, k)
	}
	for k, v := range metadata {
		d.donutMetadata[k] = v
	}
	return nil
}

func (d donutFileWriter) GetDonutMetadata() (map[string]string, error) {
	donutMetadata := make(map[string]string)
	for k, v := range d.donutMetadata {
		donutMetadata[k] = v
	}
	return donutMetadata, nil
}
