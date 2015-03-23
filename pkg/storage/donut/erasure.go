package donut

import (
	"bytes"
	"io"
	"strconv"
	"time"

	"encoding/hex"
	"errors"
	"github.com/minio-io/minio/pkg/encoding/erasure"
	"github.com/minio-io/minio/pkg/utils/crypto/sha512"
	"github.com/minio-io/minio/pkg/utils/split"
)

func erasureReader(readers []io.ReadCloser, donutMetadata map[string]string, writer *io.PipeWriter) {
	// TODO handle errors
	totalChunks, _ := strconv.Atoi(donutMetadata["chunkCount"])
	totalLeft, _ := strconv.Atoi(donutMetadata["totalLength"])
	blockSize, _ := strconv.Atoi(donutMetadata["blockSize"])
	k, _ := strconv.Atoi(donutMetadata["erasureK"])
	m, _ := strconv.Atoi(donutMetadata["erasureM"])
	expectedSha512, _ := hex.DecodeString(donutMetadata["sha512"])
	summer := sha512.New()
	// TODO select technique properly
	params, _ := erasure.ParseEncoderParams(uint8(k), uint8(m), erasure.Cauchy)
	encoder := erasure.NewEncoder(params)
	for _, reader := range readers {
		defer reader.Close()
	}
	for i := 0; i < totalChunks; i++ {
		curBlockSize := totalLeft
		if blockSize < totalLeft {
			curBlockSize = blockSize
		}
		curChunkSize := erasure.GetEncodedChunkLen(curBlockSize, uint8(k))

		encodedBytes := make([][]byte, 16)
		for i, reader := range readers {
			var bytesBuffer bytes.Buffer
			// TODO watch for errors
			io.CopyN(&bytesBuffer, reader, int64(curChunkSize))
			encodedBytes[i] = bytesBuffer.Bytes()
		}
		decodedData, err := encoder.Decode(encodedBytes, curBlockSize)
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		summer.Write(decodedData)
		io.Copy(writer, bytes.NewBuffer(decodedData))
		totalLeft = totalLeft - blockSize
	}
	actualSha512 := summer.Sum(nil)
	if bytes.Compare(expectedSha512, actualSha512) != 0 {
		writer.CloseWithError(errors.New("decoded sha512 did not match"))
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
	summer := sha512.New()
	for chunk := range chunks {
		if chunk.Err == nil {
			totalLength = totalLength + len(chunk.Data)
			encodedBlocks, _ := encoder.Encode(chunk.Data)
			summer.Write(chunk.Data)
			for blockIndex, block := range encodedBlocks {
				io.Copy(eWriter.writers[blockIndex], bytes.NewBuffer(block))
			}
		}
		chunkCount = chunkCount + 1
	}
	dataSha512 := summer.Sum(nil)
	metadata := make(map[string]string)
	metadata["blockSize"] = strconv.Itoa(10 * 1024 * 1024)
	metadata["chunkCount"] = strconv.Itoa(chunkCount)
	metadata["created"] = time.Now().Format(time.RFC3339Nano)
	metadata["erasureK"] = "8"
	metadata["erasureM"] = "8"
	metadata["erasureTechnique"] = "Cauchy"
	metadata["totalLength"] = strconv.Itoa(totalLength)
	metadata["sha512"] = hex.EncodeToString(dataSha512)
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
