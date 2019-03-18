package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const kvVolumesKey = ".minio.sys/kv-volumes"

type kvVolumes struct {
	Version  string
	VolInfos []VolInfo
}

type KVStorage struct {
	kv        KVInterface
	volumes   *kvVolumes
	path      string
	volumesMu sync.RWMutex
}

func newPosix(path string) (StorageAPI, error) {
	kvPath := path
	path = strings.TrimPrefix(path, "/ip/")

	if os.Getenv("MINIO_NKV_EMULATOR") != "" {
		dataDir := pathJoin("/tmp", path, "data")
		os.MkdirAll(dataDir, 0777)
		return &debugStorage{path, &KVStorage{kv: &KVEmulator{dataDir}, path: kvPath}, true}, nil
	}

	configPath := os.Getenv("MINIO_NKV_CONFIG")
	if configPath == "" {
		return nil, errDiskNotFound
	}

	if err := minio_nkv_open(configPath); err != nil {
		return nil, err
	}
	kv, err := newKV(path, os.Getenv("MINIO_NKV_SYNC") != "")
	if err != nil {
		return nil, err
	}
	p := &KVStorage{kv: kv, path: kvPath}
	if os.Getenv("MINIO_NKV_DEBUG") == "" {
		return p, nil
	}
	return &debugStorage{path, p, true}, nil
}

func (k *KVStorage) DataKey(id string) string {
	return path.Join(kvDataDir, id)
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
	return DiskInfo{
		Total: 3 * 1024 * 1024 * 1024 * 1024,
		Free:  3 * 1024 * 1024 * 1024 * 1024,
	}, nil
}

func (k *KVStorage) loadVolumes() (*kvVolumes, error) {
	volumes := &kvVolumes{}
	bufp := kvValuePool.Get().(*[]byte)
	defer kvValuePool.Put(bufp)

	value, err := k.kv.Get(kvVolumesKey, *bufp)
	if err != nil {
		return volumes, nil
	}
	if err = json.Unmarshal(value, volumes); err != nil {
		return nil, err
	}
	return volumes, nil
}

func (k *KVStorage) verifyVolume(volume string) error {
	_, err := k.StatVol(volume)
	return err
}

func (k *KVStorage) MakeVol(volume string) (err error) {
	k.volumesMu.Lock()
	defer k.volumesMu.Unlock()
	volumes, err := k.loadVolumes()
	if err != nil {
		return err
	}

	for _, vol := range volumes.VolInfos {
		if vol.Name == volume {
			return errVolumeExists
		}
	}

	volumes.VolInfos = append(volumes.VolInfos, VolInfo{volume, time.Now()})
	b, err := json.Marshal(volumes)
	if err != nil {
		return err
	}
	err = k.kv.Put(kvVolumesKey, b)
	if err != nil {
		return err
	}
	k.volumes = volumes
	return nil
}

func (k *KVStorage) ListVols() (vols []VolInfo, err error) {
	k.volumesMu.Lock()
	defer k.volumesMu.Unlock()
	if k.volumes == nil {
		k.volumes, err = k.loadVolumes()
		if err != nil {
			return nil, err
		}
	}
	for _, vol := range k.volumes.VolInfos {
		if vol.Name == ".minio.sys/multipart" {
			continue
		}
		if vol.Name == ".minio.sys/tmp" {
			continue
		}
		vols = append(vols, vol)
	}
	return vols, nil
}

func (k *KVStorage) StatVol(volume string) (vol VolInfo, err error) {
	k.volumesMu.Lock()
	defer k.volumesMu.Unlock()
	if k.volumes == nil {
		k.volumes, err = k.loadVolumes()
		if err != nil {
			return vol, err
		}
	}
	for _, vol := range k.volumes.VolInfos {
		if vol.Name == volume {
			return VolInfo{vol.Name, vol.Created}, nil
		}
	}
	return vol, errVolumeNotFound
}

func (k *KVStorage) DeleteVol(volume string) (err error) {
	k.volumesMu.Lock()
	defer k.volumesMu.Unlock()
	volumes, err := k.loadVolumes()
	if err != nil {
		return err
	}
	foundIndex := -1
	for i, vol := range volumes.VolInfos {
		if vol.Name == volume {
			foundIndex = i
			break
		}
	}
	if foundIndex == -1 {
		return errVolumeNotFound
	}
	volumes.VolInfos = append(volumes.VolInfos[:foundIndex], volumes.VolInfos[foundIndex+1:]...)

	b, err := json.Marshal(volumes)
	if err != nil {
		return err
	}
	err = k.kv.Put(kvVolumesKey, b)
	if err != nil {
		return err
	}
	k.volumes = volumes
	return err
}

func (k *KVStorage) getKVNSEntry(nskey string) (entry KVNSEntry, err error) {
	tries := 10
	bufp := kvValuePool.Get().(*[]byte)
	defer kvValuePool.Put(bufp)

	for {
		value, err := k.kv.Get(nskey, *bufp)
		if err != nil {
			return entry, err
		}
		err = KVNSEntryUnmarshal(value, &entry)
		if err != nil {
			fmt.Println("##### Unmarshal failed on ", nskey, len(value), string(value))
			tries--
			if tries == 0 {
				fmt.Println("##### Unmarshal failed (after 10 retries on GET) on ", k.path, nskey)
				os.Exit(0)
			}
			continue
		}
		if entry.Key != nskey {
			fmt.Printf("##### key mismatch, requested: %s, got: %s\n", nskey, entry.Key)
			tries--
			if tries == 0 {
				fmt.Printf("##### key mismatch after 10 retries, requested: %s, got: %s\n", nskey, entry.Key)
				os.Exit(0)
			}
			continue
		}
		return entry, nil
	}
}

func (k *KVStorage) ListDir(volume, dirPath string, count int) ([]string, error) {
	nskey := pathJoin(volume, dirPath, "xl.json")

	entry, err := k.getKVNSEntry(nskey)
	if err != nil {
		return nil, err
	}

	bufp := kvValuePool.Get().(*[]byte)
	defer kvValuePool.Put(bufp)

	tries := 10
	for {
		value, err := k.kv.Get(k.DataKey(entry.IDs[0]), *bufp)
		if err != nil {
			return nil, err
		}
		xlMeta, err := xlMetaV1UnmarshalJSON(context.Background(), value)
		if err != nil {
			fmt.Println("##### xlMetaV1UnmarshalJSON failed on", k.DataKey(entry.IDs[0]), len(value), string(value))
			tries--
			if tries == 0 {
				fmt.Println("##### xlMetaV1UnmarshalJSON failed on (10 retries)", k.DataKey(entry.IDs[0]), len(value), string(value))
				os.Exit(1)
			}
			continue
		}
		listEntries := []string{"xl.json"}
		for _, part := range xlMeta.Parts {
			listEntries = append(listEntries, part.Name)
		}
		return listEntries, err
	}
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
	nskey := pathJoin(volume, filePath)
	entry := KVNSEntry{Key: nskey, Size: size, ModTime: time.Now()}
	bufp := kvValuePool.Get().(*[]byte)
	defer kvValuePool.Put(bufp)

	buf := *bufp
	for {
		if size == 0 {
			break
		}
		if size < int64(len(buf)) {
			buf = buf[:size]
		}
		n, err := io.ReadFull(reader, buf)
		if err != nil {
			return err
		}
		size -= int64(n)
		id := mustGetUUID()
		if kvPadding {
			if len(buf) < kvMaxValueSize {
				paddedSize := ceilFrac(int64(len(buf)), kvNSEntryPaddingMultiple) * kvNSEntryPaddingMultiple
				for {
					if int64(len(buf)) == paddedSize {
						break
					}
					buf = append(buf, '\x00')
				}
			}
		}
		if err = k.kv.Put(k.DataKey(id), buf); err != nil {
			return err
		}
		entry.IDs = append(entry.IDs, id)
	}
	b, err := KVNSEntryMarshal(entry)
	if err != nil {
		return err
	}
	return k.kv.Put(nskey, b)
}

func (k *KVStorage) ReadFileStream(volume, filePath string, offset, length int64) (io.ReadCloser, error) {
	if err := k.verifyVolume(volume); err != nil {
		return nil, err
	}
	nskey := pathJoin(volume, filePath)
	entry, err := k.getKVNSEntry(nskey)
	if err != nil {
		return nil, err
	}

	if length != entry.Size {
		fmt.Println("length != entry.Size", length, entry.Size, nskey, entry.Key)
		fmt.Println(entry)
		return nil, fmt.Errorf("ReadFileStream: %d != %d", length, entry.Size)
	}
	if offset != 0 {
		return nil, errUnexpected
	}
	r, w := io.Pipe()
	size := entry.Size
	go func() {
		bufp := kvValuePool.Get().(*[]byte)
		defer kvValuePool.Put(bufp)

		for _, id := range entry.IDs {
			data, err := k.kv.Get(k.DataKey(id), *bufp)
			if err != nil {
				w.CloseWithError(err)
				return
			}
			if size < int64(len(data)) {
				data = data[:size]
			}
			w.Write(data)
			size -= int64(len(data))
		}
		w.Close()
	}()
	return ioutil.NopCloser(r), nil
}

func (k *KVStorage) RenameFile(srcVolume, srcPath, dstVolume, dstPath string) error {
	if err := k.verifyVolume(srcVolume); err != nil {
		return err
	}
	if err := k.verifyVolume(dstVolume); err != nil {
		return err
	}
	rename := func(src, dst string) error {
		if src == ".minio.sys/format.json.tmp" && dst == ".minio.sys/format.json" {
			bufp := kvValuePool.Get().(*[]byte)
			defer kvValuePool.Put(bufp)
			value, err := k.kv.Get(src, *bufp)
			if err != nil {
				return err
			}
			err = k.kv.Put(dst, value)
			if err != nil {
				return err
			}
			err = k.kv.Delete(src)
			return err
		}
		entry, err := k.getKVNSEntry(src)
		if err != nil {
			return err
		}
		entry.Key = dst
		value, err := KVNSEntryMarshal(entry)
		if err != nil {
			return err
		}

		err = k.kv.Put(dst, value)
		if err != nil {
			return err
		}
		err = k.kv.Delete(src)
		return err
	}
	if !strings.HasSuffix(srcPath, slashSeparator) && !strings.HasSuffix(dstPath, slashSeparator) {
		return rename(pathJoin(srcVolume, srcPath), pathJoin(dstVolume, dstPath))
	}
	if strings.HasSuffix(srcPath, slashSeparator) && strings.HasSuffix(dstPath, slashSeparator) {
		entries, err := k.ListDir(srcVolume, srcPath, -1)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err = rename(pathJoin(srcVolume, srcPath, entry), pathJoin(dstVolume, dstPath, entry)); err != nil {
				return err
			}
		}
		return nil
	}

	return errUnexpected
}

func (k *KVStorage) StatFile(volume string, path string) (fi FileInfo, err error) {
	if err := k.verifyVolume(volume); err != nil {
		return fi, err
	}
	nskey := pathJoin(volume, path)
	entry, err := k.getKVNSEntry(nskey)
	if err != nil {
		return fi, err
	}

	return FileInfo{
		Volume:  volume,
		Name:    path,
		ModTime: entry.ModTime,
		Size:    entry.Size,
		Mode:    0,
	}, nil
}

func (k *KVStorage) die(key string, err error) {
	fmt.Println("GET corrupted", k.path, key, err)
	os.Exit(1)
}

func (k *KVStorage) DeleteFile(volume string, path string) (err error) {
	if err := k.verifyVolume(volume); err != nil {
		return err
	}
	nskey := pathJoin(volume, path)
	entry, err := k.getKVNSEntry(nskey)
	if err != nil {
		return err
	}

	for _, id := range entry.IDs {
		k.kv.Delete(k.DataKey(id))
	}
	return k.kv.Delete(nskey)
}

func (k *KVStorage) WriteAll(volume string, filePath string, buf []byte) (err error) {
	if err = k.verifyVolume(volume); err != nil {
		return err
	}
	if filePath == "format.json.tmp" {
		return k.kv.Put(pathJoin(volume, filePath), buf)
	}
	return k.CreateFile(volume, filePath, int64(len(buf)), bytes.NewBuffer(buf))
}

func (k *KVStorage) ReadAll(volume string, filePath string) (buf []byte, err error) {
	if err = k.verifyVolume(volume); err != nil {
		return nil, err
	}

	if filePath == "format.json" {
		bufp := kvValuePool.Get().(*[]byte)
		defer kvValuePool.Put(bufp)

		buf, err = k.kv.Get(pathJoin(volume, filePath), *bufp)
		return buf, err
	}
	fi, err := k.StatFile(volume, filePath)
	if err != nil {
		return nil, err
	}
	r, err := k.ReadFileStream(volume, filePath, 0, fi.Size)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}
