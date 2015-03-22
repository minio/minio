package donut

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/minio-io/minio/pkg/encoding/erasure"
	"github.com/minio-io/minio/pkg/utils/split"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

type donutDriver struct {
	buckets map[string]Bucket
	nodes   map[string]Node
}

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
	buckets := make([]string, 0)
	for bucket, _ := range driver.buckets {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	return buckets, nil
}

func (driver donutDriver) GetObjectWriter(bucketName, objectName string) (ObjectWriter, error) {
	if bucket, ok := driver.buckets[bucketName]; ok == true {
		writers := make([]DonutWriter, 16)
		nodes, err := bucket.GetNodes()
		if err != nil {
			return nil, err
		}
		for i, nodeId := range nodes {
			if node, ok := driver.nodes[nodeId]; ok == true {
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
		for i, nodeId := range nodes {
			if node, ok := driver.nodes[nodeId]; ok == true {
				bucketId := bucketName + ":0:" + strconv.Itoa(i)
				reader, err := node.GetReader(bucketId, objectName)
				if err != nil {
					return nil, err
				}
				readers[i] = reader
				if metadata == nil {
					metadata, err = node.GetDonutMetadata(bucketId, objectName)
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
			var bytesArray []byte
			decoder := gob.NewDecoder(reader)
			err := decoder.Decode(&bytesArray)
			if err != nil {
				log.Println(err)
			}
			encodedBytes[i] = bytesArray
		}
		curBlockSize := totalLeft
		if blockSize < totalLeft {
			curBlockSize = blockSize
		}
		log.Println("decoding block size", curBlockSize)
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
	writers       []DonutWriter
	metadata      map[string]string
	donutMetadata map[string]string // not exposed
	erasureWriter *io.PipeWriter
	isClosed      <-chan bool
}

func newErasureWriter(writers []DonutWriter) ObjectWriter {
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
				var byteBuffer bytes.Buffer
				gobEncoder := gob.NewEncoder(&byteBuffer)
				gobEncoder.Encode(block)
				io.Copy(eWriter.writers[blockIndex], &byteBuffer)
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

func (self erasureWriter) Write(data []byte) (int, error) {
	io.Copy(self.erasureWriter, bytes.NewBuffer(data))
	return len(data), nil
}

func (self erasureWriter) Close() error {
	self.erasureWriter.Close()
	<-self.isClosed
	return nil
}

func (self erasureWriter) CloseWithError(err error) error {
	for _, writer := range self.writers {
		if writer != nil {
			writer.CloseWithError(err)
		}
	}
	return nil
}

func (self erasureWriter) SetMetadata(metadata map[string]string) error {
	for k, _ := range self.metadata {
		delete(self.metadata, k)
	}
	for k, v := range metadata {
		self.metadata[k] = v
	}
	return nil
}

func (self erasureWriter) GetMetadata() (map[string]string, error) {
	metadata := make(map[string]string)
	for k, v := range self.metadata {
		metadata[k] = v
	}
	return metadata, nil
}

type localDirectoryNode struct {
	root string
}

func (self localDirectoryNode) GetBuckets() ([]string, error) {
	return nil, errors.New("Not Implemented")
}

func (self localDirectoryNode) GetWriter(bucket, object string) (DonutWriter, error) {
	objectPath := path.Join(self.root, bucket, object)
	err := os.MkdirAll(objectPath, 0700)
	if err != nil {
		return nil, err
	}
	return newDonutFileWriter(objectPath)
}

func (self localDirectoryNode) GetReader(bucket, object string) (io.ReadCloser, error) {
	return os.Open(path.Join(self.root, bucket, object, "data"))
}

func (self localDirectoryNode) GetMetadata(bucket, object string) (map[string]string, error) {
	return self.getMetadata(bucket, object, "metadata.json")
}
func (self localDirectoryNode) GetDonutMetadata(bucket, object string) (map[string]string, error) {
	return self.getMetadata(bucket, object, "donutMetadata.json")
}

func (self localDirectoryNode) getMetadata(bucket, object, fileName string) (map[string]string, error) {
	file, err := os.Open(path.Join(self.root, bucket, object, fileName))
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

func newDonutFileWriter(objectDir string) (DonutWriter, error) {
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

func (self donutFileWriter) Write(data []byte) (int, error) {
	return self.file.Write(data)
}

func (self donutFileWriter) Close() error {
	if self.err != nil {
		return self.err
	}

	self.file.Close()

	metadata, _ := json.Marshal(self.metadata)
	ioutil.WriteFile(path.Join(self.root, "metadata.json"), metadata, 0600)
	donutMetadata, _ := json.Marshal(self.donutMetadata)
	ioutil.WriteFile(path.Join(self.root, "donutMetadata.json"), donutMetadata, 0600)

	return nil
}

func (self donutFileWriter) CloseWithError(err error) error {
	if self.err != nil {
		self.err = err
	}
	self.file.Close()
	return nil
}

func (self donutFileWriter) SetMetadata(metadata map[string]string) error {
	for k := range self.metadata {
		delete(self.metadata, k)
	}
	for k, v := range metadata {
		self.metadata[k] = v
	}
	return nil
}

func (self donutFileWriter) GetMetadata() (map[string]string, error) {
	metadata := make(map[string]string)
	for k, v := range self.metadata {
		metadata[k] = v
	}
	return metadata, nil
}

func (self donutFileWriter) SetDonutMetadata(metadata map[string]string) error {
	for k := range self.donutMetadata {
		delete(self.donutMetadata, k)
	}
	for k, v := range metadata {
		self.donutMetadata[k] = v
	}
	return nil
}

func (self donutFileWriter) GetDonutMetadata() (map[string]string, error) {
	donutMetadata := make(map[string]string)
	for k, v := range self.donutMetadata {
		donutMetadata[k] = v
	}
	return donutMetadata, nil
}
