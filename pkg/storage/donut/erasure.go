package donut

import (
	"bytes"
	"errors"
	"hash"
	"io"
	"strconv"
	"strings"
	"time"

	"crypto/md5"
	"encoding/hex"

	encoding "github.com/minio-io/erasure"
	"github.com/minio-io/iodine"
	"github.com/minio-io/minio/pkg/utils/split"
)

// getErasureTechnique - convert technique string into Technique type
func getErasureTechnique(technique string) (encoding.Technique, error) {
	switch true {
	case technique == "Cauchy":
		return encoding.Cauchy, nil
	case technique == "Vandermonde":
		return encoding.Cauchy, nil
	default:
		return encoding.None, iodine.New(errors.New("Invalid erasure technique: "+technique), nil)
	}
}

// erasureReader - returns aligned streaming reads over a PipeWriter
func erasureReader(readers []io.ReadCloser, donutMetadata map[string]string, writer *io.PipeWriter) {
	totalChunks, err := strconv.Atoi(donutMetadata["chunkCount"])
	if err != nil {
		writer.CloseWithError(iodine.New(err, donutMetadata))
		return
	}
	totalLeft, err := strconv.ParseInt(donutMetadata["size"], 10, 64)
	if err != nil {
		writer.CloseWithError(iodine.New(err, donutMetadata))
		return
	}
	blockSize, err := strconv.Atoi(donutMetadata["blockSize"])
	if err != nil {
		writer.CloseWithError(iodine.New(err, donutMetadata))
		return
	}
	parsedk, err := strconv.ParseUint(donutMetadata["erasureK"], 10, 8)
	if err != nil {
		writer.CloseWithError(iodine.New(err, donutMetadata))
		return
	}
	k := uint8(parsedk)
	parsedm, err := strconv.ParseUint(donutMetadata["erasureM"], 10, 8)
	if err != nil {
		writer.CloseWithError(iodine.New(err, donutMetadata))
		return
	}
	m := uint8(parsedm)
	expectedMd5sum, err := hex.DecodeString(donutMetadata["md5"])
	if err != nil {
		writer.CloseWithError(iodine.New(err, donutMetadata))
		return
	}
	technique, err := getErasureTechnique(donutMetadata["erasureTechnique"])
	if err != nil {
		writer.CloseWithError(iodine.New(err, donutMetadata))
		return
	}
	hasher := md5.New()
	params, err := encoding.ValidateParams(k, m, technique)
	if err != nil {
		writer.CloseWithError(iodine.New(err, donutMetadata))
	}
	encoder := encoding.NewErasure(params)
	for i := 0; i < totalChunks; i++ {
		totalLeft, err = decodeChunk(writer, readers, encoder, hasher, k, m, totalLeft, blockSize)
		if err != nil {
			errParams := map[string]string{
				"totalLeft": strconv.FormatInt(totalLeft, 10),
			}
			for k, v := range donutMetadata {
				errParams[k] = v
			}
			writer.CloseWithError(iodine.New(err, errParams))
		}
	}
	actualMd5sum := hasher.Sum(nil)
	if bytes.Compare(expectedMd5sum, actualMd5sum) != 0 {
		writer.CloseWithError(iodine.New(errors.New("decoded md5sum did not match. expected: "+string(expectedMd5sum)+" actual: "+string(actualMd5sum)), donutMetadata))
		return
	}
	writer.Close()
	return
}

func decodeChunk(writer *io.PipeWriter, readers []io.ReadCloser, encoder *encoding.Erasure, hasher hash.Hash, k, m uint8, totalLeft int64, blockSize int) (int64, error) {
	curBlockSize := 0
	if int64(blockSize) < totalLeft {
		curBlockSize = blockSize
	} else {
		curBlockSize = int(totalLeft) // cast is safe, blockSize in if protects
	}

	curChunkSize := encoding.GetEncodedBlockLen(curBlockSize, uint8(k))
	encodedBytes := make([][]byte, 16)
	for i, reader := range readers {
		var bytesBuffer bytes.Buffer
		written, err := io.CopyN(&bytesBuffer, reader, int64(curChunkSize))
		if err != nil {
			errParams := map[string]string{}
			errParams["part"] = strconv.FormatInt(written, 10)
			errParams["block.written"] = strconv.FormatInt(written, 10)
			errParams["block.length"] = strconv.Itoa(curChunkSize)
			return totalLeft, iodine.New(err, errParams)
		}
		encodedBytes[i] = bytesBuffer.Bytes()
	}
	decodedData, err := encoder.Decode(encodedBytes, curBlockSize)
	if err != nil {
		errParams := map[string]string{}
		errParams["block.length"] = strconv.Itoa(curChunkSize)
		return totalLeft, iodine.New(err, errParams)
	}
	_, err = hasher.Write(decodedData) // not expecting errors from hash, will also catch further down on .Sum mismatch in parent
	if err != nil {
		errParams := map[string]string{}
		errParams["block.length"] = strconv.Itoa(curChunkSize)
		return totalLeft, iodine.New(err, errParams)
	}
	_, err = io.Copy(writer, bytes.NewBuffer(decodedData))
	if err != nil {
		errParams := map[string]string{}
		errParams["block.length"] = strconv.Itoa(curChunkSize)
		return totalLeft, iodine.New(err, errParams)
	}
	totalLeft = totalLeft - int64(blockSize)
	return totalLeft, nil
}

// erasure writer

type erasureWriter struct {
	writers       []Writer
	metadata      map[string]string
	donutMetadata map[string]string // not exposed
	erasureWriter *io.PipeWriter
	isClosed      <-chan bool
}

// newErasureWriter - get a new writer
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
	params, _ := encoding.ValidateParams(8, 8, encoding.Cauchy)
	encoder := encoding.NewErasure(params)
	chunkCount := 0
	totalLength := 0
	summer := md5.New()
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
	dataMd5sum := summer.Sum(nil)
	metadata := make(map[string]string)
	metadata["blockSize"] = strconv.Itoa(10 * 1024 * 1024)
	metadata["chunkCount"] = strconv.Itoa(chunkCount)
	metadata["created"] = time.Now().Format(time.RFC3339Nano)
	metadata["erasureK"] = "8"
	metadata["erasureM"] = "8"
	metadata["erasureTechnique"] = "Cauchy"
	metadata["md5"] = hex.EncodeToString(dataMd5sum)
	metadata["size"] = strconv.Itoa(totalLength)
	for _, nodeWriter := range eWriter.writers {
		if nodeWriter != nil {
			nodeWriter.SetMetadata(eWriter.metadata)
			nodeWriter.SetDonutMetadata(metadata)
			nodeWriter.Close()
		}
	}
	isClosed <- true
}

func (eWriter erasureWriter) Write(data []byte) (int, error) {
	io.Copy(eWriter.erasureWriter, bytes.NewBuffer(data))
	return len(data), nil
}

func (eWriter erasureWriter) Close() error {
	eWriter.erasureWriter.Close()
	<-eWriter.isClosed
	return nil
}

func (eWriter erasureWriter) CloseWithError(err error) error {
	for _, writer := range eWriter.writers {
		if writer != nil {
			writer.CloseWithError(err)
		}
	}
	return nil
}

func (eWriter erasureWriter) SetMetadata(metadata map[string]string) error {
	for k := range metadata {
		if strings.HasPrefix(k, "sys.") {
			return errors.New("Invalid key '" + k + "', cannot start with sys.'")
		}
	}
	for k := range eWriter.metadata {
		delete(eWriter.metadata, k)
	}
	for k, v := range metadata {
		eWriter.metadata[k] = v
	}
	return nil
}

func (eWriter erasureWriter) GetMetadata() (map[string]string, error) {
	metadata := make(map[string]string)
	for k, v := range eWriter.metadata {
		metadata[k] = v
	}
	return metadata, nil
}
