package oss

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

//
// CopyFile 分片复制文件
//
// srcBucketName  源Bucket名称。
// srcObjectKey   源Object名称。
// destObjectKey   目标Object名称。目标Bucket名称为Bucket.BucketName。
// partSize   复制文件片的大小，字节数。比如100 * 1024为每片100KB。
// options    Object的属性限制项。详见InitiateMultipartUpload。
//
// error 操作成功error为nil，非nil为错误信息。
//
func (bucket Bucket) CopyFile(srcBucketName, srcObjectKey, destObjectKey string, partSize int64, options ...Option) error {
	destBucketName := bucket.BucketName
	if partSize < MinPartSize || partSize > MaxPartSize {
		return errors.New("oss: part size invalid range (1024KB, 5GB]")
	}

	cpConf, err := getCpConfig(options, filepath.Base(destObjectKey))
	if err != nil {
		return err
	}

	routines := getRoutines(options)

	if cpConf.IsEnable {
		return bucket.copyFileWithCp(srcBucketName, srcObjectKey, destBucketName, destObjectKey,
			partSize, options, cpConf.FilePath, routines)
	}

	return bucket.copyFile(srcBucketName, srcObjectKey, destBucketName, destObjectKey,
		partSize, options, routines)
}

// ----- 并发无断点的下载  -----

// 工作协程参数
type copyWorkerArg struct {
	bucket        *Bucket
	imur          InitiateMultipartUploadResult
	srcBucketName string
	srcObjectKey  string
	options       []Option
	hook          copyPartHook
}

// Hook用于测试
type copyPartHook func(part copyPart) error

var copyPartHooker copyPartHook = defaultCopyPartHook

func defaultCopyPartHook(part copyPart) error {
	return nil
}

// 工作协程
func copyWorker(id int, arg copyWorkerArg, jobs <-chan copyPart, results chan<- UploadPart, failed chan<- error, die <-chan bool) {
	for chunk := range jobs {
		if err := arg.hook(chunk); err != nil {
			failed <- err
			break
		}
		chunkSize := chunk.End - chunk.Start + 1
		part, err := arg.bucket.UploadPartCopy(arg.imur, arg.srcBucketName, arg.srcObjectKey,
			chunk.Start, chunkSize, chunk.Number, arg.options...)
		if err != nil {
			failed <- err
			break
		}
		select {
		case <-die:
			return
		default:
		}
		results <- part
	}
}

// 调度协程
func copyScheduler(jobs chan copyPart, parts []copyPart) {
	for _, part := range parts {
		jobs <- part
	}
	close(jobs)
}

// 分片
type copyPart struct {
	Number int   // 片序号[1, 10000]
	Start  int64 // 片起始位置
	End    int64 // 片结束位置
}

// 文件分片
func getCopyParts(bucket *Bucket, objectKey string, partSize int64) ([]copyPart, error) {
	meta, err := bucket.GetObjectDetailedMeta(objectKey)
	if err != nil {
		return nil, err
	}

	parts := []copyPart{}
	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 0)
	if err != nil {
		return nil, err
	}

	part := copyPart{}
	i := 0
	for offset := int64(0); offset < objectSize; offset += partSize {
		part.Number = i + 1
		part.Start = offset
		part.End = GetPartEnd(offset, objectSize, partSize)
		parts = append(parts, part)
		i++
	}
	return parts, nil
}

// 获取源文件大小
func getSrcObjectBytes(parts []copyPart) int64 {
	var ob int64
	for _, part := range parts {
		ob += (part.End - part.Start + 1)
	}
	return ob
}

// 并发无断点续传的下载
func (bucket Bucket) copyFile(srcBucketName, srcObjectKey, destBucketName, destObjectKey string,
	partSize int64, options []Option, routines int) error {
	descBucket, err := bucket.Client.Bucket(destBucketName)
	srcBucket, err := bucket.Client.Bucket(srcBucketName)
	listener := getProgressListener(options)

	// 分割文件
	parts, err := getCopyParts(srcBucket, srcObjectKey, partSize)
	if err != nil {
		return err
	}

	// 初始化上传任务
	imur, err := descBucket.InitiateMultipartUpload(destObjectKey, options...)
	if err != nil {
		return err
	}

	jobs := make(chan copyPart, len(parts))
	results := make(chan UploadPart, len(parts))
	failed := make(chan error)
	die := make(chan bool)

	var completedBytes int64
	totalBytes := getSrcObjectBytes(parts)
	event := newProgressEvent(TransferStartedEvent, 0, totalBytes)
	publishProgress(listener, event)

	// 启动工作协程
	arg := copyWorkerArg{descBucket, imur, srcBucketName, srcObjectKey, options, copyPartHooker}
	for w := 1; w <= routines; w++ {
		go copyWorker(w, arg, jobs, results, failed, die)
	}

	// 并发上传分片
	go copyScheduler(jobs, parts)

	// 等待分片下载完成
	completed := 0
	ups := make([]UploadPart, len(parts))
	for completed < len(parts) {
		select {
		case part := <-results:
			completed++
			ups[part.PartNumber-1] = part
			completedBytes += (parts[part.PartNumber-1].End - parts[part.PartNumber-1].Start + 1)
			event = newProgressEvent(TransferDataEvent, completedBytes, totalBytes)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			descBucket.AbortMultipartUpload(imur)
			event = newProgressEvent(TransferFailedEvent, completedBytes, totalBytes)
			publishProgress(listener, event)
			return err
		}

		if completed >= len(parts) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, totalBytes)
	publishProgress(listener, event)

	// 提交任务
	_, err = descBucket.CompleteMultipartUpload(imur, ups)
	if err != nil {
		bucket.AbortMultipartUpload(imur)
		return err
	}
	return nil
}

// ----- 并发有断点的下载  -----

const copyCpMagic = "84F1F18C-FF1D-403B-A1D8-9DEB5F65910A"

type copyCheckpoint struct {
	Magic          string       // magic
	MD5            string       // cp内容的MD5
	SrcBucketName  string       // 源Bucket
	SrcObjectKey   string       // 源Object
	DestBucketName string       // 目标Bucket
	DestObjectKey  string       // 目标Bucket
	CopyID         string       // copy id
	ObjStat        objectStat   // 文件状态
	Parts          []copyPart   // 全部分片
	CopyParts      []UploadPart // 分片上传成功后的返回值
	PartStat       []bool       // 分片下载是否完成
}

// CP数据是否有效，CP有效且Object没有更新时有效
func (cp copyCheckpoint) isValid(bucket *Bucket, objectKey string) (bool, error) {
	// 比较CP的Magic及MD5
	cpb := cp
	cpb.MD5 = ""
	js, _ := json.Marshal(cpb)
	sum := md5.Sum(js)
	b64 := base64.StdEncoding.EncodeToString(sum[:])

	if cp.Magic != downloadCpMagic || b64 != cp.MD5 {
		return false, nil
	}

	// 确认object没有更新
	meta, err := bucket.GetObjectDetailedMeta(objectKey)
	if err != nil {
		return false, err
	}

	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 0)
	if err != nil {
		return false, err
	}

	// 比较Object的大小/最后修改时间/etag
	if cp.ObjStat.Size != objectSize ||
		cp.ObjStat.LastModified != meta.Get(HTTPHeaderLastModified) ||
		cp.ObjStat.Etag != meta.Get(HTTPHeaderEtag) {
		return false, nil
	}

	return true, nil
}

// 从文件中load
func (cp *copyCheckpoint) load(filePath string) error {
	contents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(contents, cp)
	return err
}

// 更新分片状态
func (cp *copyCheckpoint) update(part UploadPart) {
	cp.CopyParts[part.PartNumber-1] = part
	cp.PartStat[part.PartNumber-1] = true
}

// dump到文件
func (cp *copyCheckpoint) dump(filePath string) error {
	bcp := *cp

	// 计算MD5
	bcp.MD5 = ""
	js, err := json.Marshal(bcp)
	if err != nil {
		return err
	}
	sum := md5.Sum(js)
	b64 := base64.StdEncoding.EncodeToString(sum[:])
	bcp.MD5 = b64

	// 序列化
	js, err = json.Marshal(bcp)
	if err != nil {
		return err
	}

	// dump
	return ioutil.WriteFile(filePath, js, FilePermMode)
}

// 未完成的分片
func (cp copyCheckpoint) todoParts() []copyPart {
	dps := []copyPart{}
	for i, ps := range cp.PartStat {
		if !ps {
			dps = append(dps, cp.Parts[i])
		}
	}
	return dps
}

// 完成的字节数
func (cp copyCheckpoint) getCompletedBytes() int64 {
	var completedBytes int64
	for i, part := range cp.Parts {
		if cp.PartStat[i] {
			completedBytes += (part.End - part.Start + 1)
		}
	}
	return completedBytes
}

// 初始化下载任务
func (cp *copyCheckpoint) prepare(srcBucket *Bucket, srcObjectKey string, destBucket *Bucket, destObjectKey string,
	partSize int64, options []Option) error {
	// cp
	cp.Magic = copyCpMagic
	cp.SrcBucketName = srcBucket.BucketName
	cp.SrcObjectKey = srcObjectKey
	cp.DestBucketName = destBucket.BucketName
	cp.DestObjectKey = destObjectKey

	// object
	meta, err := srcBucket.GetObjectDetailedMeta(srcObjectKey)
	if err != nil {
		return err
	}

	objectSize, err := strconv.ParseInt(meta.Get(HTTPHeaderContentLength), 10, 0)
	if err != nil {
		return err
	}

	cp.ObjStat.Size = objectSize
	cp.ObjStat.LastModified = meta.Get(HTTPHeaderLastModified)
	cp.ObjStat.Etag = meta.Get(HTTPHeaderEtag)

	// parts
	cp.Parts, err = getCopyParts(srcBucket, srcObjectKey, partSize)
	if err != nil {
		return err
	}
	cp.PartStat = make([]bool, len(cp.Parts))
	for i := range cp.PartStat {
		cp.PartStat[i] = false
	}
	cp.CopyParts = make([]UploadPart, len(cp.Parts))

	// init copy
	imur, err := destBucket.InitiateMultipartUpload(destObjectKey, options...)
	if err != nil {
		return err
	}
	cp.CopyID = imur.UploadID

	return nil
}

func (cp *copyCheckpoint) complete(bucket *Bucket, parts []UploadPart, cpFilePath string) error {
	imur := InitiateMultipartUploadResult{Bucket: cp.DestBucketName,
		Key: cp.DestObjectKey, UploadID: cp.CopyID}
	_, err := bucket.CompleteMultipartUpload(imur, parts)
	if err != nil {
		return err
	}
	os.Remove(cpFilePath)
	return err
}

// 并发带断点的下载
func (bucket Bucket) copyFileWithCp(srcBucketName, srcObjectKey, destBucketName, destObjectKey string,
	partSize int64, options []Option, cpFilePath string, routines int) error {
	descBucket, err := bucket.Client.Bucket(destBucketName)
	srcBucket, err := bucket.Client.Bucket(srcBucketName)
	listener := getProgressListener(options)

	// LOAD CP数据
	ccp := copyCheckpoint{}
	err = ccp.load(cpFilePath)
	if err != nil {
		os.Remove(cpFilePath)
	}

	// LOAD出错或数据无效重新初始化下载
	valid, err := ccp.isValid(srcBucket, srcObjectKey)
	if err != nil || !valid {
		if err = ccp.prepare(srcBucket, srcObjectKey, descBucket, destObjectKey, partSize, options); err != nil {
			return err
		}
		os.Remove(cpFilePath)
	}

	// 未完成的分片
	parts := ccp.todoParts()
	imur := InitiateMultipartUploadResult{
		Bucket:   destBucketName,
		Key:      destObjectKey,
		UploadID: ccp.CopyID}

	jobs := make(chan copyPart, len(parts))
	results := make(chan UploadPart, len(parts))
	failed := make(chan error)
	die := make(chan bool)

	completedBytes := ccp.getCompletedBytes()
	event := newProgressEvent(TransferStartedEvent, completedBytes, ccp.ObjStat.Size)
	publishProgress(listener, event)

	// 启动工作协程
	arg := copyWorkerArg{descBucket, imur, srcBucketName, srcObjectKey, options, copyPartHooker}
	for w := 1; w <= routines; w++ {
		go copyWorker(w, arg, jobs, results, failed, die)
	}

	// 并发下载分片
	go copyScheduler(jobs, parts)

	// 等待分片下载完成
	completed := 0
	for completed < len(parts) {
		select {
		case part := <-results:
			completed++
			ccp.update(part)
			ccp.dump(cpFilePath)
			completedBytes += (parts[part.PartNumber-1].End - parts[part.PartNumber-1].Start + 1)
			event = newProgressEvent(TransferDataEvent, completedBytes, ccp.ObjStat.Size)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			event = newProgressEvent(TransferFailedEvent, completedBytes, ccp.ObjStat.Size)
			publishProgress(listener, event)
			return err
		}

		if completed >= len(parts) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, ccp.ObjStat.Size)
	publishProgress(listener, event)

	return ccp.complete(descBucket, ccp.CopyParts, cpFilePath)
}
