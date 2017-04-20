package cmd

import (
	"io/ioutil"
	"testing"
)

func TestDiskCache(t *testing.T) {
	cacheDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewDiskCache(cacheDir, 80, 1)
	if err != nil {
		t.Fatal(err)
	}
	bucketName := "testbucket"
	objectName := "testobject"
	content := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	etag := "061208c10af71a30c6dcd6cf5d89f0fe"
	contentType := "application/zip"
	// modTime := time.Now()
	size := len(content)

	httpMeta := make(map[string]string)

	objInfo := ObjectInfo{}
	objInfo.Bucket = bucketName
	objInfo.Name = objectName
	objInfo.Size = int64(size)
	objInfo.ContentType = contentType
	objInfo.MD5Sum = etag
	objInfo.UserDefined = httpMeta

	cacheObject, err := cache.Put()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cacheObject.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err = cacheObject.Commit(objInfo, false); err != nil {
		t.Fatal(err)
	}
	cacheObject, gotObjInfo, anon, err := cache.Get(bucketName, objectName)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := ioutil.ReadAll(cacheObject)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != content {
		t.Fatal(err)
	}
	if anon != false {
		t.Fatal("Expected anon to be false")
	}
	if objInfo.MD5Sum != gotObjInfo.MD5Sum {
		t.Fatal(`httpMeta["ETag"] != httpMetaStored["ETag"]`)
	}
	if objInfo.ContentType != gotObjInfo.ContentType {
		t.Fatal(`httpMeta["Content-Type"] != httpMetaStored["Content-Type"]`)
	}
	if err = cache.Delete(bucketName, objectName); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = cache.Get(bucketName, objectName); err == nil {
		t.Fatal("Object should have been deleted from the cache")
	}

	cacheObject, err = cache.Put()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cacheObject.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err = cacheObject.NoCommit(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = cache.Get(bucketName, objectName); err == nil {
		t.Fatal("Object should not be available the cache")
	}
}
