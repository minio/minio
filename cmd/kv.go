package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type KV struct {
	path string
}

type KVNSEntry struct {
	Size int64
	IDs  []string
}

var errValueTooLong = errors.New("value too long")

const kvDataDir = ".minio.sys/.data"
const kvMaxValueSize = 2 * 1024 * 1024

func (k *KV) Put(key string, value []byte) error {
	if len(value) > kvMaxValueSize {
		return errValueTooLong
	}
	fullPath := pathJoin(k.path, key)
	os.MkdirAll(path.Dir(fullPath), 0755)
	return ioutil.WriteFile(pathJoin(k.path, key), value, 0644)
}

func (k *KV) Get(key string) ([]byte, error) {
	st, err := os.Stat(pathJoin(k.path, key))
	if err != nil {
		// File is really not found.
		if os.IsNotExist(err) {
			return nil, errFileNotFound
		} else if isSysErrNotDir(err) {
			// File path cannot be verified since one of the parents is a file.
			return nil, errFileNotFound
		}

		// Return all errors here.
		return nil, err
	}
	// If its a directory its not a regular file.
	if st.Mode().IsDir() {
		return nil, errFileNotFound
	}
	return ioutil.ReadFile(pathJoin(k.path, key))
}

func (k *KV) Delete(key string) error {
	return os.Remove(pathJoin(k.path, key))
}

func (k *KV) DataKey(id string) string {
	return path.Join(kvDataDir, id)
}

func (k *KV) CreateFile(nskey string, size int64, reader io.Reader) error {
	entry := KVNSEntry{Size: size}
	buf := make([]byte, kvMaxValueSize)
	for {
		if size < int64(len(buf)) {
			buf = buf[:size]
		}
		n, err := io.ReadFull(reader, buf)
		if err != nil {
			return err
		}
		size -= int64(n)
		id := mustGetUUID()
		if err = k.Put(k.DataKey(id), buf); err != nil {
			return err
		}
		entry.IDs = append(entry.IDs, id)
		if size == 0 {
			break
		}
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return k.Put(nskey, b)
}

func (k *KV) ReadFileStream(nskey string, offset, size int64) (io.ReadCloser, error) {
	entry := KVNSEntry{}
	b, err := k.Get(nskey)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, &entry); err != nil {
		return nil, err
	}
	if size != entry.Size {
		return nil, errUnexpected
	}
	if offset != 0 {
		return nil, errUnexpected
	}
	r, w := io.Pipe()
	go func() {
		for _, id := range entry.IDs {
			data, err := k.Get(k.DataKey(id))
			if err != nil {
				w.CloseWithError(err)
				return
			}
			w.Write(data)
		}
		w.Close()
	}()
	return ioutil.NopCloser(r), nil
}

func (k *KV) DeleteFile(nskey string) error {
	entry := KVNSEntry{}
	b, err := k.Get(nskey)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &entry); err != nil {
		return err
	}
	for _, id := range entry.IDs {
		k.Delete(k.DataKey(id))
	}
	return k.Delete(nskey)
}

func (k *KV) StatFile(nskey string) (size int64, modTime time.Time, err error) {
	entry := KVNSEntry{}
	b, err := k.Get(nskey)
	if err != nil {
		return
	}
	if err = json.Unmarshal(b, &entry); err != nil {
		return
	}
	fi, err := os.Stat(pathJoin(k.path, nskey))
	if err != nil {
		return
	}
	return entry.Size, fi.ModTime(), nil
}

func (k *KV) ListDir(nskey string) ([]string, error) {
	entry := KVNSEntry{}
	b, err := k.Get(nskey)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, &entry); err != nil {
		return nil, err
	}
	b, err = k.Get(k.DataKey(entry.IDs[0]))
	if err != nil {
		return nil, err
	}
	xlMeta, err := xlMetaV1UnmarshalJSON(context.Background(), b)
	if err != nil {
		return nil, err
	}
	listEntries := []string{"xl.json"}
	for _, part := range xlMeta.Parts {
		listEntries = append(listEntries, part.Name)
	}
	return listEntries, err
}

type KVStorage struct {
	kv   *KV
	path string
}

func newPosix(path string) (StorageAPI, error) {
	os.MkdirAll(path, 0777)
	os.MkdirAll(pathJoin(path, kvVolumeDir), 0777)

	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	p := &KVStorage{&KV{path}, path}
	if strings.HasSuffix(path, "export-xl/disk1") {
		return &debugStorage{path, p}, nil
	}
	return p, nil
}

func (k *KVStorage) String() string {
	return k.path
}

func (k *KVStorage) IsOnline() bool {
	return true
}

func (k *KVStorage) LastError() error {
	return nil
}

func (k *KVStorage) Close() error {
	return nil
}

func (k *KVStorage) DiskInfo() (info DiskInfo, err error) {
	di, err := getDiskInfo(k.path)
	if err != nil {
		return info, err
	}

	return DiskInfo{
		Total: di.Total,
		Free:  di.Free,
	}, nil
}

const kvVolumeDir = ".volumes"

func (k *KVStorage) getVolumeDir(volume string) string {
	if strings.HasPrefix(volume, minioMetaBucket) {
		return pathJoin(k.path, volume)
	}
	return pathJoin(k.path, kvVolumeDir, volume)
}

func (k *KVStorage) verifyVolume(volume string) error {
	volumeDir := k.getVolumeDir(volume)
	_, err := os.Stat(volumeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return errVolumeNotFound
		}
		return err
	}
	return nil
}

func (k *KVStorage) MakeVol(volume string) (err error) {
	volumeDir := k.getVolumeDir(volume)
	_, err = os.Stat(volumeDir)
	if err == nil {
		return errVolumeExists
	}
	return os.MkdirAll(volumeDir, 0777)
}

func (k *KVStorage) ListVols() (vols []VolInfo, err error) {
	vols, err = listVols(pathJoin(k.path, kvVolumeDir))
	return vols, err
}

func (k *KVStorage) StatVol(volume string) (vol VolInfo, err error) {
	fi, err := os.Stat(k.getVolumeDir(volume))
	if err != nil {
		if os.IsNotExist(err) {
			return VolInfo{}, errVolumeNotFound
		} else if isSysErrIO(err) {
			return VolInfo{}, errFaultyDisk
		}
		return VolInfo{}, err
	}
	return VolInfo{volume, fi.ModTime()}, nil
}

func (k *KVStorage) DeleteVol(volume string) (err error) {
	err = os.Remove(k.getVolumeDir(volume))
	if err != nil {
		switch {
		case os.IsNotExist(err):
			return errVolumeNotFound
		case isSysErrNotEmpty(err):
			return errVolumeNotEmpty
		case os.IsPermission(err):
			return errDiskAccessDenied
		case isSysErrIO(err):
			return errFaultyDisk
		default:
			return err
		}
	}
	return nil
}

func (k *KVStorage) ListDir(volume, dirPath string, count int) ([]string, error) {
	return k.kv.ListDir(pathJoin(volume, dirPath, "xl.json"))
}

func (k *KVStorage) ReadFile(volume string, path string, offset int64, buf []byte, verifier *BitrotVerifier) (n int64, err error) {
	if err = k.verifyVolume(volume); err != nil {
		return 0, err
	}
	return 0, errFileAccessDenied
}

func (k *KVStorage) AppendFile(volume string, path string, buf []byte) (err error) {
	if err = k.verifyVolume(volume); err != nil {
		return err
	}
	return errFileAccessDenied
}

func (k *KVStorage) CreateFile(volume, filePath string, size int64, reader io.Reader) error {
	if err := k.verifyVolume(volume); err != nil {
		return err
	}
	return k.kv.CreateFile(pathJoin(volume, filePath), size, reader)
}

func (k *KVStorage) ReadFileStream(volume, filePath string, offset, length int64) (io.ReadCloser, error) {
	if err := k.verifyVolume(volume); err != nil {
		return nil, err
	}
	return k.kv.ReadFileStream(pathJoin(volume, filePath), offset, length)
}

func (k *KVStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	if err := k.verifyVolume(srcVolume); err != nil {
		return err
	}
	if err := k.verifyVolume(dstVolume); err != nil {
		return err
	}
	err := renameAll(path.Join(k.path, srcVolume, srcPath), path.Join(k.path, dstVolume, dstPath))
	return err
}

func (k *KVStorage) StatFile(volume string, path string) (fi FileInfo, err error) {
	if err := k.verifyVolume(volume); err != nil {
		return fi, err
	}
	size, modTime, err := k.kv.StatFile(pathJoin(volume, path))
	if err != nil {
		return fi, err
	}
	return FileInfo{
		Volume:  volume,
		Name:    path,
		ModTime: modTime,
		Size:    size,
		Mode:    0,
	}, nil
}

func (k *KVStorage) DeleteFile(volume string, path string) (err error) {
	if err := k.verifyVolume(volume); err != nil {
		return err
	}
	return k.kv.DeleteFile(pathJoin(volume, path))
}

func (k *KVStorage) WriteAll(volume string, filePath string, buf []byte) (err error) {
	if err = k.verifyVolume(volume); err != nil {
		return err
	}
	if filePath == "format.json.tmp" {
		return ioutil.WriteFile(pathJoin(k.path, volume, filePath), buf, 0644)
	}
	return k.CreateFile(volume, filePath, int64(len(buf)), bytes.NewBuffer(buf))
}

func (k *KVStorage) ReadAll(volume string, filePath string) (buf []byte, err error) {
	if err = k.verifyVolume(volume); err != nil {
		return nil, err
	}

	if filePath == "format.json" {
		buf, err = ioutil.ReadFile(pathJoin(k.path, volume, filePath))
		if err != nil {
			switch {
			case os.IsNotExist(err):
				return nil, errFileNotFound
			case os.IsPermission(err):
				return nil, errFileAccessDenied
			case isSysErrNotDir(err):
				return nil, errFileAccessDenied
			case isSysErrIO(err):
				return nil, errFaultyDisk
			default:
				return nil, err
			}
		}
		return buf, err

	}
	nsKey := pathJoin(volume, filePath)
	size, _, err := k.kv.StatFile(nsKey)
	if err != nil {
		return nil, err
	}
	r, err := k.kv.ReadFileStream(nsKey, 0, size)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}
