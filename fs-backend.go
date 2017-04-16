package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type FileSystemBackend struct {
	rootDir string
}

func (fs FileSystemBackend) ListBuckets() (bucketInfo []BucketInfo, err error) {
	fis, err := readDir(fs.rootDir)
	if err == nil {
		for _, fi := range fis {
			if fi.IsDir() {
				bucketInfo = append(bucketInfo, BucketInfo{Name: fi.Name(), ModifiedTime: fi.ModTime()})
			}
		}
	}

	return
}

func (fs FileSystemBackend) IsBucketExist(bucket string) (status bool, err error) {
	fi, err := os.Lstat(filepath.Join(fs.rootDir, bucket))
	if err == nil {
		if fi.IsDir() {
			status = true
		}
	}

	return
}

func (fs FileSystemBackend) MakeBucket(bucket string) error {
	return os.Mkdir(filepath.Join(fs.rootDir, bucket), os.ModeDir|os.ModePerm)
}

func (fs FileSystemBackend) RemoveBucket(bucket string) error {
	return os.Remove(filepath.Join(fs.rootDir, bucket))
}

func (fs FileSystemBackend) ListObjects(bucket, prefix string) (<-chan ObjectInfo, error) {
	bucketDir := filepath.Join(fs.rootDir, bucket)
	if status, err := IsDirExist(bucketDir); !status {
		if err == nil {
			return nil, errors.New(bucketDir + " does not exist")
		} else {
			return nil, err
		}
	}

	prefixDir := filepath.Dir(strings.Replace(prefix, "/", PathSeparatorString, -1))
	scanDir := filepath.Join(bucketDir, prefixDir)

	ois, err := filteredReadDir(scanDir, bucketDir, prefix)
	if err != nil {
		return nil, err
	}

	objectInfoCh := make(chan ObjectInfo, 1000)

	go func() {
		defer close(objectInfoCh)
		for len(ois) > 0 {
			oi := ois[0]
			ois = ois[1:]
			objectInfoCh <- oi

			if oi.IsDir {
				scanDir := filepath.Join(bucketDir, strings.Replace(oi.Name, "/", PathSeparatorString, -1))
				subois, err := filteredReadDir(scanDir, bucketDir, prefix)
				if err != nil {
					objectInfoCh <- ObjectInfo{Err: err}
					break
				}

				if len(subois) != 0 {
					ois = append(subois, ois...)
				}
			}
		}
	}()

	return objectInfoCh, nil
}

func (fs FileSystemBackend) ListIncompleteUploads(bucket, prefix string) (<-chan UploadInfo, error) {
	return nil, nil
}

func (fs FileSystemBackend) ListParts(bucket, object, uploadID string) (<-chan PartInfo, error) {
	return nil, nil
}

func (fs FileSystemBackend) PutObject(bucket, object string, reader io.Reader) error {
	return nil
}

func (fs FileSystemBackend) GetObject(bucket, object string, offset, length int64) (io.Reader, error) {
	return nil, nil
}

func (fs FileSystemBackend) RemoveObject(bucket, object string) error {
	objectPath := strings.Replace(object, "/", string(os.PathSeparator), -1)
	if err := os.Remove(filepath.Join(fs.rootDir, bucket, objectPath)); err == nil {
		for objectPath = filepath.Dir(objectPath); objectPath != "."; objectPath = filepath.Dir(objectPath) {
			path := filepath.Join(fs.rootDir, bucket, objectPath)
			if ok, err := IsDirEmpty(path); ok {
				if err = os.Remove(path); err != nil {
					return err
				}
			}
		}

		return err
	} else {
		return err
	}
}

func (fs FileSystemBackend) RecoverObject(bucket, object string) error {
	return nil
}

func (fs FileSystemBackend) StatObject(bucket, object string) (info ObjectInfo, err error) {
	objectPath := strings.Replace(object, "/", string(os.PathSeparator), -1)
	fi, err := os.Lstat(filepath.Join(fs.rootDir, bucket, objectPath))
	if err == nil {
		if fi.Mode().IsRegular() {
			info = ObjectInfo{
				Name:         object,
				ModifiedTime: fi.ModTime(),
				Checksum:     "",
				Size:         fi.Size(),
			}
		} else {
			err = errors.New("object is not a regular file")
		}
	}

	return
}

func NewFileSystemBackend(rootDir string) Backend {
	return FileSystemBackend{rootDir}
}
