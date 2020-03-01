package hbase

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/pkg/errors"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
)

const (
	metaTableSuffix   = "meta"
	dataTableSuffix   = "data"
	columnFamily      = "cf"
	qualifier         = "value"
	maxObjectSize     = 10485760 - 65
	defaultShardLevel = 1
)

var (
	defaultCF = map[string]map[string]string{
		columnFamily: nil,
	}
)

type Client interface {
	// Need to support both directory and object.
	PutObject(ctx context.Context, bucket string, object string, reader io.Reader) (os.FileInfo, error)
	GetObject(ctx context.Context, bucket string, object string) (io.ReaderAt, error)
	DeleteObject(ctx context.Context, bucket string, object string) error
	GetObjectInfo(ctx context.Context, bucket string, object string) (os.FileInfo, error)
	ListObjects(ctx context.Context, bucket string, prefix string) ([]os.FileInfo, error)

	CreateMultipartObject(ctx context.Context, bucket string, object string, purgeOld bool) error
	PutMultipartObjectPart(ctx context.Context, bucket string, object string, partID int, reader io.Reader) (os.FileInfo, error)
	PutMultipartObjectComplete(ctx context.Context, bucket string, object string) (os.FileInfo, error)
	PutMultipartObjectAbort(ctx context.Context, bucket string, object string) error

	DeleteBucket(ctx context.Context, bucket string) error
	CreateBucket(ctx context.Context, bucket string) error
	GetBucketInfo(ctx context.Context, bucket string) (os.FileInfo, error)
	ListBuckets(ctx context.Context) ([]os.FileInfo, error)

	Close(ctx context.Context) error
}

type hbaseClient struct {
	client      gohbase.Client
	adminClient gohbase.AdminClient
}

func NewHBaseClient(zkAddr string) Client {
	return &hbaseClient{
		client:      gohbase.NewClient(zkAddr),
		adminClient: gohbase.NewAdminClient(zkAddr),
	}
}

func (h *hbaseClient) ListObjects(ctx context.Context, bucket string, prefix string) ([]os.FileInfo, error) {
	shardLevel, err := h.getShardLevel(bucket)
	if err != nil {
		return nil, errors.Wrapf(err, "get shard level for bucket '%v' failed", bucket)
	}

	var fis []os.FileInfo
	for i := 0; i < int(math.Pow(16, float64(shardLevel))); i++ {
		_fis, err := h.listMetaInfosByShardAndPrefix(ctx, shardLevel, i, bucket, prefix)
		if err != nil {
			return nil, errors.Wrapf(err, "list meta infos with prefix '%v' at shard '%v' failed", prefix, i)
		}
		fis = append(fis, _fis...)
	}

	return fis, nil
}

func (h *hbaseClient) PutObject(ctx context.Context, bucket string, object string, reader io.Reader) (os.FileInfo, error) {
	isEnd := false
	buf := make([]byte, maxObjectSize)
	partNo := 0
	var size int64 = 0

	for !isEnd {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return nil, errors.Wrapf(err, "read from source when put object '%v' in bucket '%v' failed", object, bucket)
		}

		if err != nil && err == io.EOF {
			isEnd = true
		}

		_buf := buf[0:n]

		if len(_buf) > 0 {
			err = h.putData(ctx, bucket, object, partNo, _buf)
			if err != nil {
				return nil, errors.Wrapf(err, "put object '%v' in bucket '%v' at '%v' failed", object, bucket, partNo)
			}
			partNo += 1
			size += int64(n)
		}
	}

	parts := partNo

	// Clear previous data.
	found := true
	for found {
		err := h.deleteData(ctx, bucket, object, partNo)
		partNo += 1
		if err != nil {
			found = false
		}
	}

	isDir := false
	if size == 0 && strings.HasSuffix(object, hbaseSeparator) {
		isDir = true
	}

	fi := NewFileInfo(object, size, time.Now(), parts, isDir)
	err := h.putMetaInfo(ctx, bucket, object, fi)
	if err != nil {
		return nil, errors.Wrapf(err, "put the meta info of object '%v' in bucket '%v' failed", object, bucket)
	}

	return fi, nil
}

func (h *hbaseClient) GetObject(ctx context.Context, bucket string, object string) (io.ReaderAt, error) {
	fi, err := h.getMetaInfo(ctx, bucket, object)
	if err != nil {
		return nil, errors.Wrapf(err, "get meta info for bucket '%v' and object '%v' failed", bucket, object)
	}

	reader := NewReader(ctx, bucket, object, fi.Size(), fi.parts, h)

	return reader, nil
}

func (h *hbaseClient) GetObjectInfo(ctx context.Context, bucket string, object string) (os.FileInfo, error) {
	fi, err := h.getMetaInfo(ctx, bucket, object)
	if err != nil {
		return nil, errors.Wrapf(err, "get meta info for object '%v' in bucket '%v' failed", bucket, object)
	}
	return fi, nil
}

func (h *hbaseClient) DeleteObject(ctx context.Context, bucket string, object string) error {
	fi, err := h.getMetaInfo(ctx, bucket, object)
	if err != nil {
		return errors.Wrapf(err, "get meta info for object '%v' in bucket '%v' failed when delete object", bucket, object)
	}

	err = h.deleteMetaInfo(ctx, bucket, object)
	if err != nil {
		if !strings.Contains(err.Error(), "could not find key") {
			return errors.Wrapf(err, "delete meta info for object '%v' in bucket '%v' failed", bucket, object)
		}
	}

	partNo := 0
	found := true
	for found {
		err := h.deleteData(ctx, bucket, object, partNo)
		partNo += 1
		if partNo > fi.parts && err != nil {
			found = false
		}
	}

	return nil
}

func (h *hbaseClient) getShardLevel(bucket string) (int, error) {
	// TODO: Change to read the meta info of bucket first.
	return defaultShardLevel, nil
}

func (h *hbaseClient) getKeySplitsByShardLevel(shardLevel int) [][]byte {
	shardCount := int(math.Pow(16, float64(shardLevel)))
	keySplits := make([][]byte, 0, shardCount)

	for i := 0; i < shardCount; i++ {
		keySplits = append(keySplits, []byte(fmt.Sprintf(fmt.Sprintf("%%0%dx", shardLevel), i)))
	}

	return keySplits
}

func (h *hbaseClient) getMetaInfo(ctx context.Context, bucket string, object string) (*FileInfo, error) {
	shardLevel, err := h.getShardLevel(bucket)
	if err != nil {
		return nil, errors.Wrapf(err, "get shard level for bucket '%v' failed", bucket)
	}

	table := h.getMetaTableFromBucket(bucket)
	key := h.getMetaInfoKey(shardLevel, object)
	data, err := h.getKey(ctx, table, key)
	if err != nil {
		return nil, errors.Wrapf(err, "read key '%v' in table '%v' failed", key, table)
	}

	var fi FileInfo
	err = json.Unmarshal(data, &fi)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshal '%s' to file info failed", data)
	}

	return &fi, nil
}

func (h *hbaseClient) listMetaInfosByShardAndPrefix(ctx context.Context, shardLevel int, shardNo int, bucket string, prefix string) ([]os.FileInfo, error) {
	table := h.getMetaTableFromBucket(bucket)
	keyPrefix := fmt.Sprintf("%v_%v", fmt.Sprintf(fmt.Sprintf("%%0%dx", shardLevel), shardNo), prefix)

	data, err := h.scanKey(ctx, table, keyPrefix)
	if err != nil {
		return nil, errors.Wrapf(err, "scale '%v' in table '%v' failed", keyPrefix, table)
	}

	fis := make([]os.FileInfo, 0, len(data))
	for _, value := range data {
		var fi FileInfo
		err = json.Unmarshal(value, &fi)
		if err != nil {
			logger.LogIf(ctx, err)

			continue
		}

		fis = append(fis, &fi)
	}

	return fis, nil
}

func (h *hbaseClient) putMetaInfo(ctx context.Context, bucket string, object string, fi *FileInfo) error {
	shardLevel, err := h.getShardLevel(bucket)
	if err != nil {
		return errors.Wrapf(err, "get shard level for bucket '%v' failed", bucket)
	}

	data, err := json.Marshal(fi)
	if err != nil {
		return errors.Wrapf(err, "marshal '%v' to file info failed", fi)
	}

	table := h.getMetaTableFromBucket(bucket)
	key := h.getMetaInfoKey(shardLevel, object)
	err = h.putKey(ctx, table, key, data)
	if err != nil {
		return errors.Wrapf(err, "put key '%v' with value '%s' in table '%v' failed", key, data, table)
	}

	return nil
}

func (h *hbaseClient) deleteMetaInfo(ctx context.Context, bucket string, object string) error {
	shardLevel, err := h.getShardLevel(bucket)
	if err != nil {
		return errors.Wrapf(err, "get shard level for bucket '%v' failed", bucket)
	}

	table := h.getMetaTableFromBucket(bucket)
	key := h.getMetaInfoKey(shardLevel, object)
	err = h.deleteKey(ctx, table, key)
	if err != nil {
		return errors.Wrapf(err, "delete key '%v' in table '%v' failed", key, table)
	}

	return nil
}

func (h *hbaseClient) getData(ctx context.Context, bucket string, object string, partNo int) ([]byte, error) {
	table := h.getDataTableFromBucket(bucket)
	key := h.getDataKey(object, partNo)
	return h.getKey(ctx, table, key)
}

func (h *hbaseClient) putData(ctx context.Context, bucket string, object string, partNo int, data []byte) error {
	table := h.getDataTableFromBucket(bucket)
	key := h.getDataKey(object, partNo)
	return h.putKey(ctx, table, key, data)
}

func (h *hbaseClient) deleteData(ctx context.Context, bucket string, object string, partNo int) error {
	table := h.getDataTableFromBucket(bucket)
	key := h.getDataKey(object, partNo)
	return h.deleteKey(ctx, table, key)
}

func (h *hbaseClient) deleteKey(ctx context.Context, table string, key string) error {
	family := map[string]map[string][]byte{columnFamily: nil}
	delReq, err := hrpc.NewDelStr(ctx, table, key, family)
	if err != nil {
		return errors.Wrapf(err, "create del request for table '%v' and key '%v' failed", table, key)
	}

	delResp, err := h.client.Delete(delReq)
	if err != nil {
		return errors.Wrapf(err, "get del response for table '%v' and key '%v' failed", table, key)
	}

	if delResp.Exists == nil || !*delResp.Exists {
		return errors.Errorf("could not find key '%v' in table '%v'", key, table)
	}

	return nil
}

func (h *hbaseClient) getKey(ctx context.Context, table string, key string) ([]byte, error) {
	family := map[string][]string{columnFamily: {qualifier}}
	getReq, err := hrpc.NewGetStr(ctx, table, key, hrpc.Families(family))
	if err != nil {
		return nil, errors.Wrapf(err, "create get request for table '%v' and key '%v' failed", table, key)
	}

	getResp, err := h.client.Get(getReq)
	if err != nil {
		return nil, errors.Wrapf(err, "get get response for table '%v' and key '%v' failed", table, key)
	}

	if len(getResp.Cells) == 0 {
		return nil, errors.Errorf("could not find key '%v' in table '%v'", key, table)
	}

	return getResp.Cells[0].Value, nil
}

func (h *hbaseClient) scanKey(ctx context.Context, table string, keyPrefix string) ([][]byte, error) {
	prefixFilter := filter.NewPrefixFilter([]byte(keyPrefix))
	scanReq, err := hrpc.NewScanStr(ctx, table, hrpc.Filters(prefixFilter))
	if err != nil {
		return nil, errors.Wrapf(err, "create scan request for table '%v' and key prefix '%v' failed", table, keyPrefix)
	}

	var data [][]byte
	scanResp := h.client.Scan(scanReq)
	for {
		row, err := scanResp.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.LogIf(ctx, err)
			break
		}

		data = append(data, row.Cells[0].Value)
	}

	return data, nil
}

func (h *hbaseClient) putKey(ctx context.Context, table string, key string, data []byte) error {
	values := map[string]map[string][]byte{columnFamily: {qualifier: data}}
	putReq, err := hrpc.NewPutStr(ctx, table, key, values)
	if err != nil {
		return errors.Wrapf(err, "create put request for table '%v' and key '%v' failed", table, key)
	}

	_, err = h.client.Put(putReq)
	if err != nil {
		return errors.Wrapf(err, "get put response for table '%v' and key '%v' failed", table, key)
	}

	return nil
}

func (h *hbaseClient) CreateMultipartObject(ctx context.Context, bucket string, object string, purgeOld bool) error {
	if purgeOld {
		if err := h.DeleteObject(ctx, bucket, object); err != nil {
			if !strings.Contains(err.Error(), "could not find") {
				return errors.Wrapf(err, "delete object '%v' in bucket '%v' failed", object, bucket)
			}
		}
	}

	fi := NewFileInfo(object, 0, time.Now(), 0, false)
	err := h.putMetaInfo(ctx, bucket, object, fi)
	if err != nil {
		return errors.Wrapf(err, "put meta info for object '%v' in bucket '%v' failed", object, bucket)
	}

	return nil
}

func (h *hbaseClient) PutMultipartObjectPart(ctx context.Context, bucket string, object string, partID int, reader io.Reader) (os.FileInfo, error) {
	fi, err := h.getMetaInfo(ctx, bucket, object)
	if err != nil {
		return nil, errors.Wrapf(err, "get meta info for bucket '%v' and object '%v' failed", bucket, object)
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read data for bucket '%v' and object '%v' failed", bucket, object)
	}

	err = h.putData(ctx, bucket, object, fi.parts, data)
	if err != nil {
		return nil, errors.Wrapf(err, "put data for bucket '%v' and object '%v' failed", bucket, object)
	}

	fi.modTime = time.Now()
	fi.parts += 1
	fi.size += int64(len(data))
	if err := h.putMetaInfo(ctx, bucket, object, fi); err != nil {
		return nil, errors.Wrapf(err, "put meta info for bucket '%v' and object '%v' failed", bucket, object)
	}

	return fi, nil
}

// No need to implement.
func (h *hbaseClient) PutMultipartObjectComplete(ctx context.Context, bucket string, object string) (os.FileInfo, error) {
	return nil, nil
}

// No need to implement.
func (h *hbaseClient) PutMultipartObjectAbort(ctx context.Context, bucket string, object string) error {
	return nil
}

func (h *hbaseClient) DeleteBucket(ctx context.Context, bucket string) error {
	metaTable := h.getMetaTableFromBucket(bucket)
	dataTable := h.getDataTableFromBucket(bucket)

	_ = h.adminClient.DisableTable(hrpc.NewDisableTable(ctx, []byte(metaTable)))

	if err := h.adminClient.DeleteTable(hrpc.NewDeleteTable(ctx, []byte(metaTable))); err != nil {
		if !strings.Contains(err.Error(), "TableNotFoundException") {
			return errors.Wrapf(err, "delete meta table '%v' failed", metaTable)
		}
	}

	_ = h.adminClient.DisableTable(hrpc.NewDisableTable(ctx, []byte(dataTable)))

	if err := h.adminClient.DeleteTable(hrpc.NewDeleteTable(ctx, []byte(dataTable))); err != nil {
		if !strings.Contains(err.Error(), "TableNotFoundException") {
			return errors.Wrapf(err, "delete data table '%v' failed", dataTable)
		}
	}

	return nil
}

func (h *hbaseClient) CreateBucket(ctx context.Context, bucket string) error {
	metaTable := h.getMetaTableFromBucket(bucket)
	dataTable := h.getDataTableFromBucket(bucket)

	shardLevel, err := h.getShardLevel(bucket)
	if err != nil {
		return errors.Wrapf(err, "get shard level for bucket '%v' failed", bucket)
	}

	keySplits := h.getKeySplitsByShardLevel(shardLevel)

	if err := h.adminClient.CreateTable(
		hrpc.NewCreateTable(ctx, []byte(metaTable), defaultCF, hrpc.SplitKeys(keySplits)),
	); err != nil {
		if !strings.Contains(err.Error(), "org.apache.hadoop.hbase.TableExistsException") {
			return errors.Wrapf(err, "create meta table '%v' failed", err)
		}
	}

	if err := h.adminClient.CreateTable(
		hrpc.NewCreateTable(ctx, []byte(dataTable), defaultCF, hrpc.SplitKeys(keySplits)),
	); err != nil {
		if !strings.Contains(err.Error(), "org.apache.hadoop.hbase.TableExistsException") {
			return errors.Wrapf(err, "create data table '%v' failed", err)
		}
	}

	return nil
}

func (h *hbaseClient) GetBucketInfo(ctx context.Context, bucket string) (os.FileInfo, error) {
	tables, err := h.adminClient.GetTableNames(hrpc.NewGetTableNames(ctx, h.getMetaTableFromBucket(bucket)))
	if err != nil {
		return nil, errors.Wrapf(err, "get tables failed")
	}

	if len(tables) == 0 {
		return nil, errors.Wrapf(err, "could not found bucket '%v'", bucket)
	}

	return NewFileInfo(bucket, 0, time.Now(), 0, true), nil
}

func (h *hbaseClient) ListBuckets(ctx context.Context) ([]os.FileInfo, error) {
	tables, err := h.adminClient.GetTableNames(hrpc.NewGetTableNames(ctx, ".*"))
	if err != nil {
		return nil, errors.Wrapf(err, "get tables failed")
	}

	names := make(map[string]bool)
	for _, table := range tables {
		name := h.parseBucketFromTable(string(table.Qualifier))
		if len(name) > 0 {
			names[name] = true
		}
	}

	fis := make([]os.FileInfo, 0, len(names))
	for name := range names {
		fi := NewFileInfo(name, 0, time.Now(), 0, true)
		fis = append(fis, fi)
	}

	return fis, nil
}

func (h *hbaseClient) parseBucketFromTable(table string) string {
	splits := strings.Split(table, "_")
	if len(splits) >= 2 && splits[len(splits)-1] == metaTableSuffix {
		return strings.Join(splits[0:len(splits)-1], "_")
	}

	return ""
}

func (h *hbaseClient) getMetaTableFromBucket(bucket string) string {
	return fmt.Sprintf("%v_%v", bucket, metaTableSuffix)
}

func (h *hbaseClient) getDataTableFromBucket(bucket string) string {
	return fmt.Sprintf("%v_%v", bucket, dataTableSuffix)
}

func (h *hbaseClient) getDataKey(object string, partNo int) string {
	hasher := md5.New()
	hasher.Write([]byte(object))
	hash := hex.EncodeToString(hasher.Sum(nil))

	return fmt.Sprintf("%v_%v", hash, partNo)
}

func (h *hbaseClient) getMetaInfoKey(shardLevel int, object string) string {
	hasher := md5.New()
	hasher.Write([]byte(object))
	hash := hasher.Sum(nil)

	return fmt.Sprintf("%v_%v", hex.EncodeToString(hash[0:shardLevel])[0:shardLevel], object)
}

func (h *hbaseClient) Close(ctx context.Context) error {
	h.client.Close()
	return nil
}

type FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	parts   int
	isDir   bool
}

func NewFileInfo(name string, size int64, modTime time.Time, parts int, isDir bool) *FileInfo {
	return &FileInfo{
		name:    name,
		size:    size,
		mode:    os.FileMode(0777),
		modTime: modTime,
		parts:   parts,
		isDir:   isDir,
	}
}

func (f *FileInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Name    string `json:"name"`
		Size    int64  `json:"size"`
		IsDir   bool   `json:"is_dir"`
		ModTime int64  `json:"mod_time"`
		Parts   int    `json:"parts"`
	}{
		Name:    f.name,
		Size:    f.size,
		IsDir:   f.isDir,
		ModTime: f.modTime.Unix(),
		Parts:   f.parts,
	})
}

func (f *FileInfo) UnmarshalJSON(data []byte) error {
	_f := &struct {
		Name    string `json:"name"`
		Size    int64  `json:"size"`
		IsDir   bool   `json:"is_dir"`
		ModTime int64  `json:"mod_time"`
		Parts   int    `json:"parts"`
	}{}

	if err := json.Unmarshal(data, _f); err != nil {
		return errors.Wrapf(err, "unmarshal '%s' to file info failed", data)
	}

	f.name = _f.Name
	f.size = _f.Size
	f.isDir = _f.IsDir
	f.modTime = time.Unix(_f.ModTime, 0)
	f.parts = _f.Parts

	return nil
}

func (f *FileInfo) Name() string {
	return f.name
}

func (f *FileInfo) Size() int64 {
	return f.size
}

func (f *FileInfo) Mode() os.FileMode {
	return f.mode
}

func (f *FileInfo) ModTime() time.Time {
	return f.modTime
}

func (f *FileInfo) IsDir() bool {
	return f.isDir
}

func (f *FileInfo) Sys() interface{} {
	return nil
}

type Reader struct {
	ctx    context.Context
	bucket string
	object string
	size   int64
	parts  int
	client *hbaseClient
}

func NewReader(ctx context.Context, bucket string, object string, size int64, parts int, client *hbaseClient) *Reader {
	return &Reader{
		ctx:    ctx,
		bucket: bucket,
		object: object,
		size:   size,
		parts:  parts,
		client: client,
	}
}

func (r *Reader) ReadAt(b []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("bytes.Reader.ReadAt: negative offset")
	}
	// if off >= r.size {
	// 	return 0, io.EOF
	// }

	n = 0
	for i := 0; i < r.parts; i++ {
		data, err := r.client.getData(r.ctx, r.bucket, r.object, i)
		if err != nil {
			return 0, errors.Wrapf(err, "get data from object '%v' at partNo '%v' in bucket '%v' failed", r.object, i, r.bucket)
		}

		if off >= int64(len(data)) {
			off -= int64(len(data))
			continue
		}

		n += copy(b, data[off:])
		off = 0
	}

	if n < len(b) {
		err = io.EOF
	}

	return
}
