package encodedstorage

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	"github.com/minio-io/minio/pkgs/erasure"
	"github.com/minio-io/minio/pkgs/split"
	"github.com/minio-io/minio/pkgs/storage"
	"github.com/minio-io/minio/pkgs/storage/appendstorage"
	"github.com/spaolacci/murmur3"
)

type encodedStorage struct {
	RootDir     string
	K           int
	M           int
	BlockSize   uint64
	objects     map[string]StorageEntry
	diskStorage []storage.ObjectStorage
}

type StorageEntry struct {
	Path          string
	Md5sum        []byte
	Murmurhash    uint64
	Blocks        []StorageBlockEntry
	Encoderparams erasure.EncoderParams
}

type StorageBlockEntry struct {
	Index  int
	Length int
}

type storeRequest struct {
	path string
	data []byte
}

type storeResponse struct {
	data []byte
	err  error
}

func NewStorage(rootDir string, k, m int, blockSize uint64) (storage.ObjectStorage, error) {
	// create storage files
	if k == 0 || m == 0 {
		return nil, errors.New("Invalid protection level")
	}

	storageNodes := make([]storage.ObjectStorage, k+m)
	for i := 0; i < k+m; i++ {
		storageNode, err := appendstorage.NewStorage(rootDir, i)
		storageNodes[i] = storageNode
		if err != nil {
			return nil, err
		}
	}
	objects := make(map[string]StorageEntry)
	indexPath := path.Join(rootDir, "index")
	if _, err := os.Stat(indexPath); err == nil {
		indexFile, err := os.Open(indexPath)
		defer indexFile.Close()
		if err != nil {
			return nil, err
		}
		encoder := gob.NewDecoder(indexFile)
		err = encoder.Decode(&objects)
		if err != nil && err != io.EOF {
			return nil, err
		}
	}
	newStorage := encodedStorage{
		RootDir:     rootDir,
		K:           k,
		M:           m,
		BlockSize:   blockSize,
		objects:     objects,
		diskStorage: storageNodes,
	}
	return &newStorage, nil
}

func (eStorage *encodedStorage) Get(objectPath string) (io.Reader, error) {
	entry, ok := eStorage.objects[objectPath]
	if ok == false {
		return nil, nil
	}
	reader, writer := io.Pipe()
	go eStorage.readObject(objectPath, entry, writer)
	return reader, nil
}

func (eStorage *encodedStorage) List() ([]storage.ObjectDescription, error) {
	var objectDescList []storage.ObjectDescription
	for objectName, objectEntry := range eStorage.objects {
		var objectDescription storage.ObjectDescription
		protectionLevel := strconv.Itoa(objectEntry.Encoderparams.K) + "," + strconv.Itoa(objectEntry.Encoderparams.M)
		objectDescription.Name = objectName
		objectDescription.Md5sum = hex.EncodeToString(objectEntry.Md5sum)
		objectDescription.Hash = strconv.FormatUint(objectEntry.Murmurhash, 16)
		objectDescription.Protectionlevel = protectionLevel
		objectDescList = append(objectDescList, objectDescription)
	}
	if len(objectDescList) == 0 {
		return nil, errors.New("No objects found")
	}
	return objectDescList, nil
}

func (eStorage *encodedStorage) Put(objectPath string, object io.Reader) error {
	// split
	chunks := make(chan split.SplitMessage)
	go split.SplitStream(object, eStorage.BlockSize, chunks)

	// for each chunk
	encoderParameters, err := erasure.ParseEncoderParams(eStorage.K, eStorage.M, erasure.CAUCHY)
	if err != nil {
		return err
	}
	encoder := erasure.NewEncoder(encoderParameters)
	entry := StorageEntry{
		Path:       objectPath,
		Md5sum:     nil,
		Murmurhash: 0,
		Blocks:     make([]StorageBlockEntry, 0),
		Encoderparams: erasure.EncoderParams{
			K:         eStorage.K,
			M:         eStorage.M,
			Technique: erasure.CAUCHY,
		},
	}
	// Hash
	murmur := murmur3.Sum64([]byte(objectPath))
	// allocate md5
	hash := md5.New()
	i := 0
	// encode
	for chunk := range chunks {
		if chunk.Err == nil {
			// encode each
			blocks, length := encoder.Encode(chunk.Data)
			// store each
			storeErrors := eStorage.storeBlocks(objectPath+"$"+strconv.Itoa(i), blocks)
			for _, err := range storeErrors {
				if err != nil {
					return err
				}
			}
			// md5sum only after chunk is committed to disk
			hash.Write(chunk.Data)
			blockEntry := StorageBlockEntry{
				Index:  i,
				Length: length,
			}
			entry.Blocks = append(entry.Blocks, blockEntry)
		} else {
			return chunk.Err
		}
		i++
	}
	entry.Md5sum = hash.Sum(nil)
	entry.Murmurhash = murmur
	eStorage.objects[objectPath] = entry
	var gobBuffer bytes.Buffer
	gobEncoder := gob.NewEncoder(&gobBuffer)
	gobEncoder.Encode(eStorage.objects)
	ioutil.WriteFile(path.Join(eStorage.RootDir, "index"), gobBuffer.Bytes(), 0600)
	return nil
}

func (eStorage *encodedStorage) storeBlocks(path string, blocks [][]byte) []error {
	returnChannels := make([]<-chan error, len(eStorage.diskStorage))
	for i, store := range eStorage.diskStorage {
		returnChannels[i] = storageRoutine(store, path, bytes.NewBuffer(blocks[i]))
	}
	returnErrors := make([]error, 0)
	for _, returnChannel := range returnChannels {
		for returnValue := range returnChannel {
			if returnValue != nil {
				returnErrors = append(returnErrors, returnValue)
			}
		}
	}
	return returnErrors
}

func (eStorage *encodedStorage) readObject(objectPath string, entry StorageEntry, writer *io.PipeWriter) {
	ep, err := erasure.ParseEncoderParams(entry.Encoderparams.K, entry.Encoderparams.M, entry.Encoderparams.Technique)
	if err != nil {
		writer.CloseWithError(err)
		return
	}
	encoder := erasure.NewEncoder(ep)
	for i, chunk := range entry.Blocks {
		blockSlices := eStorage.getBlockSlices(objectPath + "$" + strconv.Itoa(i))
		if len(blockSlices) == 0 {
			writer.CloseWithError(errors.New("slices missing!!"))
			return
		}
		var blocks [][]byte
		for _, slice := range blockSlices {
			if slice.err != nil {
				writer.CloseWithError(slice.err)
				return
			}
			blocks = append(blocks, slice.data)
		}
		data, err := encoder.Decode(blocks, chunk.Length)
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		bytesWritten := 0
		for bytesWritten != len(data) {
			written, err := writer.Write(data[bytesWritten:len(data)])
			if err != nil {
				writer.CloseWithError(err)
			}
			bytesWritten += written
		}
	}
	writer.Close()
}

func (eStorage *encodedStorage) getBlockSlices(objectPath string) []storeResponse {
	responses := make([]<-chan storeResponse, 0)
	for i := 0; i < len(eStorage.diskStorage); i++ {
		response := getSlice(eStorage.diskStorage[i], objectPath)
		responses = append(responses, response)
	}
	results := make([]storeResponse, 0)
	for _, response := range responses {
		results = append(results, <-response)
	}
	return results
}

func getSlice(store storage.ObjectStorage, path string) <-chan storeResponse {
	out := make(chan storeResponse)
	go func() {
		obj, err := store.Get(path)
		if err != nil {
			out <- storeResponse{data: nil, err: err}
		} else {
			data, err := ioutil.ReadAll(obj)
			out <- storeResponse{data: data, err: err}
		}
		close(out)
	}()
	return out
}

func storageRoutine(store storage.ObjectStorage, path string, data io.Reader) <-chan error {
	out := make(chan error)
	go func() {
		if err := store.Put(path, data); err != nil {
			out <- err
		}
		close(out)
	}()
	return out
}
