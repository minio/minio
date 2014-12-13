package fsstorage

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

	"github.com/minio-io/minio/pkgs/split"
	"github.com/minio-io/minio/pkgs/storage"
	"github.com/minio-io/minio/pkgs/storage/appendstorage"
)

type fileSystemStorage struct {
	RootDir     string
	BlockSize   uint64
	diskStorage []storage.ObjectStorage
	objects     map[string]StorageEntry
}

type StorageEntry struct {
	Path        string
	Md5sum      []byte
	ChunkLength int
}

func NewStorage(rootDir string, blockSize uint64) (storage.ObjectStorage, error) {
	var storageNodes []storage.ObjectStorage
	storageNode, err := appendstorage.NewStorage(rootDir, 0)
	if err != nil {
		return nil, err
	}
	storageNodes = append(storageNodes, storageNode)
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
	newStorage := fileSystemStorage{
		RootDir:     rootDir,
		diskStorage: storageNodes,
		BlockSize:   blockSize,
		objects:     objects,
	}
	return &newStorage, nil
}

func (fsStorage *fileSystemStorage) List() ([]storage.ObjectDescription, error) {
	var objectDescList []storage.ObjectDescription
	for objectName, objectEntry := range fsStorage.objects {
		var objectDescription storage.ObjectDescription
		objectDescription.Name = objectName
		objectDescription.Md5sum = hex.EncodeToString(objectEntry.Md5sum)
		objectDescription.Protectionlevel = ""
		objectDescList = append(objectDescList, objectDescription)
	}
	if len(objectDescList) == 0 {
		return nil, errors.New("No objects found")
	}
	return objectDescList, nil
}

func (fsStorage *fileSystemStorage) Get(objectPath string) (io.Reader, error) {
	entry, ok := fsStorage.objects[objectPath]
	if ok == false {
		return nil, nil
	}
	reader, writer := io.Pipe()
	go fsStorage.readObject(objectPath, entry, writer)
	return reader, nil
}

func (fsStorage *fileSystemStorage) readObject(objectPath string, entry StorageEntry, writer *io.PipeWriter) {
	appendStorage := fsStorage.diskStorage[0]
	for i := 0; i < entry.ChunkLength; i++ {
		chunkObjectPath := objectPath + "$" + strconv.Itoa(i)
		chunkObject, err := appendStorage.Get(chunkObjectPath)

		if err != nil {
			writer.CloseWithError(err)
		}
		data, readErr := ioutil.ReadAll(chunkObject)

		if readErr != nil {
			writer.CloseWithError(readErr)
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

func (fsStorage *fileSystemStorage) Put(objectPath string, object io.Reader) error {
	// split
	chunks := make(chan split.SplitMessage)
	go split.SplitStream(object, fsStorage.BlockSize, chunks)

	entry := StorageEntry{
		Path:        objectPath,
		Md5sum:      nil,
		ChunkLength: 0,
	}

	hash := md5.New()
	i := 0
	for chunk := range chunks {
		if chunk.Err != nil {
			return chunk.Err
		}
		err := fsStorage.storeBlocks(objectPath, i, chunk.Data)
		if err != nil {
			return err
		}
		// md5sum only after chunk is committed to disk
		hash.Write(chunk.Data)
		i++
	}
	entry.Md5sum = hash.Sum(nil)
	entry.ChunkLength = i
	fsStorage.objects[objectPath] = entry
	var gobBuffer bytes.Buffer
	gobEncoder := gob.NewEncoder(&gobBuffer)
	gobEncoder.Encode(fsStorage.objects)
	ioutil.WriteFile(path.Join(fsStorage.RootDir, "index"), gobBuffer.Bytes(), 0600)
	return nil
}

func (fsStorage *fileSystemStorage) storeBlocks(objectPath string, index int, chunk []byte) error {
	appendStorage := fsStorage.diskStorage[0]
	path := objectPath + "$" + strconv.Itoa(index)
	if err := appendStorage.Put(path, bytes.NewBuffer(chunk)); err != nil {
		return err
	}
	return nil
}
