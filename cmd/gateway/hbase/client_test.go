package hbase

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestBucketOperations(t *testing.T) {
	ctx := context.Background()

	client := NewHBaseClient("localhost")
	bucket := "test_registry"
	err := client.CreateBucket(ctx, bucket)
	if err != nil {
		t.Fatalf("create_bucket_failed|err=%v", err)
	}

	fi, err := client.GetBucketInfo(ctx, bucket)
	if err != nil {
		t.Fatalf("get_bucket_info_failed|err=%v", err)
	} else {
		t.Logf("get_bucket_info_success|fi=%v", fi)
	}

	fis, err := client.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("list_buckets_failed|err=%v", err)
	} else {
		t.Logf("list_buckets_success|fi=%v", fis[0])
	}

	err = client.DeleteBucket(ctx, bucket)
	if err != nil {
		t.Fatalf("delete_bucket_failed|err=%v", err)
	}
}

func TestObjectOperations(t *testing.T) {
	ctx := context.Background()

	client := NewHBaseClient("localhost")
	bucket := "test_registry"
	err := client.CreateBucket(ctx, bucket)
	if err != nil {
		t.Fatalf("create_bucket_failed|err=%v", err)
	} else {
		t.Logf("create_bucket_success|bucket=%v", bucket)
	}

	object := "/a/b.txt"
	reader := bytes.NewReader([]byte("hello"))
	fi, err := client.PutObject(ctx, bucket, object, reader)
	if err != nil {
		t.Fatalf("put_object_failed|err=%v", err)
	} else {
		t.Logf("put_object_success|fi=%v", fi)
	}

	readerAt, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		t.Fatalf("get_object_failed|err=%v", err)
	} else {
		p := make([]byte, 100)
		n, err := readerAt.ReadAt(p, 2)
		if err != nil && err != io.EOF {
			t.Fatalf("read_at_when_get_object_failed|err=%v", err)
		} else {
			t.Logf("read_at_when_get_object_success|data=%s,n=%v", p, n)
		}
	}

	fi, err = client.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		t.Fatalf("get_object_info_failed|err=%v", err)
	} else {
		t.Logf("get_object_info_success|fi=%v", fi)
	}

	fis, err := client.ListObjects(ctx, bucket, "/")
	if err != nil {
		t.Fatalf("list_objects_failed|err=%v", err)
	} else {
		t.Logf("list_objects_success|fis=%v", fis[0])
	}
}

func TestMultipartObjectOperations(t *testing.T) {
	ctx := context.Background()

	client := NewHBaseClient("localhost")
	bucket := "test_registry"
	err := client.CreateBucket(ctx, bucket)
	if err != nil {
		t.Fatalf("create_bucket_failed|err=%v", err)
	} else {
		t.Logf("create_bucket_success|bucket=%v", bucket)
	}

	object := "/a/b.txt"
	reader := bytes.NewReader([]byte("world"))
	err = client.CreateMultipartObject(ctx, bucket, object, true)
	if err != nil {
		t.Fatalf("create_multi_part_object_failed|err=%v", err)
	} else {
		t.Logf("create_multi_part_object")
	}

	fi, err := client.PutMultipartObjectPart(ctx, bucket, object, 0, reader)
	if err != nil {
		t.Fatalf("put_multi_part_object_failed|err=%v", err)
	} else {
		t.Logf("put_multi_part_object_failed|fi=%v", fi)
	}

	readerAt, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		t.Fatalf("get_object_failed|err=%v", err)
	} else {
		p := make([]byte, 100)
		n, err := readerAt.ReadAt(p, 2)
		if err != nil && err != io.EOF {
			t.Fatalf("read_at_when_get_object_failed|err=%v", err)
		} else {
			t.Logf("read_at_when_get_object_success|data=%s,n=%v", p, n)
		}
	}
}
