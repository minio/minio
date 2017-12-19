package oss

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"time"
)

//
// UploadFile 分片上传文件
//
// objectKey  object名称。
// filePath   本地文件。需要上传的文件。
// partSize   本次上传文件片的大小，字节数。比如100 * 1024为每片100KB。
// options    上传Object时可以指定Object的属性。详见InitiateMultipartUpload。
//
// error 操作成功为nil，非nil为错误信息。
//
func (bucket Bucket) UploadFile(objectKey, filePath string, partSize int64, options ...Option) error {
	if partSize < MinPartSize || partSize > MaxPartSize {
		return errors.New("oss: part size invalid range (1024KB, 5GB]")
	}

	cpConf, err := getCpConfig(options, filePath)
	if err != nil {
		return err
	}

	routines := getRoutines(options)

	if cpConf.IsEnable {
		return bucket.uploadFileWithCp(objectKey, filePath, partSize, options, cpConf.FilePath, routines)
	}

	return bucket.uploadFile(objectKey, filePath, partSize, options, routines)
}

// ----- 并发无断点的上传  -----

// 获取Checkpoint配置
func getCpConfig(options []Option, filePath string) (*cpConfig, error) {
	cpc := &cpConfig{}
	cpcOpt, err := findOption(options, checkpointConfig, nil)
	if err != nil || cpcOpt == nil {
		return cpc, err
	}

	cpc = cpcOpt.(*cpConfig)
	if cpc.IsEnable && cpc.FilePath == "" {
		cpc.FilePath = filePath + CheckpointFileSuffix
	}

	return cpc, nil
}

// 获取并发数，默认并发数1
func getRoutines(options []Option) int {
	rtnOpt, err := findOption(options, routineNum, nil)
	if err != nil || rtnOpt == nil {
		return 1
	}

	rs := rtnOpt.(int)
	if rs < 1 {
		rs = 1
	} else if rs > 100 {
		rs = 100
	}

	return rs
}

// 获取进度回调
func getProgressListener(options []Option) ProgressListener {
	isSet, listener, _ := isOptionSet(options, progressListener)
	if !isSet {
		return nil
	}
	return listener.(ProgressListener)
}

// 测试使用
type uploadPartHook func(id int, chunk FileChunk) error

var uploadPartHooker uploadPartHook = defaultUploadPart

func defaultUploadPart(id int, chunk FileChunk) error {
	return nil
}

// 工作协程参数
type workerArg struct {
	bucket   *Bucket
	filePath string
	imur     InitiateMultipartUploadResult
	hook     uploadPartHook
}

// 工作协程
func worker(id int, arg workerArg, jobs <-chan FileChunk, results chan<- UploadPart, failed chan<- error, die <-chan bool) {
	for chunk := range jobs {
		if err := arg.hook(id, chunk); err != nil {
			failed <- err
			break
		}
		part, err := arg.bucket.UploadPartFromFile(arg.imur, arg.filePath, chunk.Offset, chunk.Size, chunk.Number)
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
func scheduler(jobs chan FileChunk, chunks []FileChunk) {
	for _, chunk := range chunks {
		jobs <- chunk
	}
	close(jobs)
}

func getTotalBytes(chunks []FileChunk) int64 {
	var tb int64
	for _, chunk := range chunks {
		tb += chunk.Size
	}
	return tb
}

// 并发上传，不带断点续传功能
func (bucket Bucket) uploadFile(objectKey, filePath string, partSize int64, options []Option, routines int) error {
	listener := getProgressListener(options)

	chunks, err := SplitFileByPartSize(filePath, partSize)
	if err != nil {
		return err
	}

	// 初始化上传任务
	imur, err := bucket.InitiateMultipartUpload(objectKey, options...)
	if err != nil {
		return err
	}

	jobs := make(chan FileChunk, len(chunks))
	results := make(chan UploadPart, len(chunks))
	failed := make(chan error)
	die := make(chan bool)

	var completedBytes int64
	totalBytes := getTotalBytes(chunks)
	event := newProgressEvent(TransferStartedEvent, 0, totalBytes)
	publishProgress(listener, event)

	// 启动工作协程
	arg := workerArg{&bucket, filePath, imur, uploadPartHooker}
	for w := 1; w <= routines; w++ {
		go worker(w, arg, jobs, results, failed, die)
	}

	// 并发上传分片
	go scheduler(jobs, chunks)

	// 等待分配分片上传完成
	completed := 0
	parts := make([]UploadPart, len(chunks))
	for completed < len(chunks) {
		select {
		case part := <-results:
			completed++
			parts[part.PartNumber-1] = part
			completedBytes += chunks[part.PartNumber-1].Size
			event = newProgressEvent(TransferDataEvent, completedBytes, totalBytes)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			event = newProgressEvent(TransferFailedEvent, completedBytes, totalBytes)
			publishProgress(listener, event)
			bucket.AbortMultipartUpload(imur)
			return err
		}

		if completed >= len(chunks) {
			break
		}
	}

	event = newProgressEvent(TransferStartedEvent, completedBytes, totalBytes)
	publishProgress(listener, event)

	// 提交任务
	_, err = bucket.CompleteMultipartUpload(imur, parts)
	if err != nil {
		bucket.AbortMultipartUpload(imur)
		return err
	}
	return nil
}

// ----- 并发带断点的上传  -----
const uploadCpMagic = "FE8BB4EA-B593-4FAC-AD7A-2459A36E2E62"

type uploadCheckpoint struct {
	Magic     string   // magic
	MD5       string   // cp内容的MD5
	FilePath  string   // 本地文件
	FileStat  cpStat   // 文件状态
	ObjectKey string   // key
	UploadID  string   // upload id
	Parts     []cpPart // 本地文件的全部分片
}

type cpStat struct {
	Size         int64     // 文件大小
	LastModified time.Time // 本地文件最后修改时间
	MD5          string    // 本地文件MD5
}

type cpPart struct {
	Chunk       FileChunk  // 分片
	Part        UploadPart // 上传完成的分片
	IsCompleted bool       // upload是否完成
}

// CP数据是否有效，CP有效且文件没有更新时有效
func (cp uploadCheckpoint) isValid(filePath string) (bool, error) {
	// 比较CP的Magic及MD5
	cpb := cp
	cpb.MD5 = ""
	js, _ := json.Marshal(cpb)
	sum := md5.Sum(js)
	b64 := base64.StdEncoding.EncodeToString(sum[:])

	if cp.Magic != uploadCpMagic || b64 != cp.MD5 {
		return false, nil
	}

	// 确认本地文件是否更新
	fd, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer fd.Close()

	st, err := fd.Stat()
	if err != nil {
		return false, err
	}

	md, err := calcFileMD5(filePath)
	if err != nil {
		return false, err
	}

	// 比较文件大小/文件最后更新时间/文件MD5
	if cp.FileStat.Size != st.Size() ||
		cp.FileStat.LastModified != st.ModTime() ||
		cp.FileStat.MD5 != md {
		return false, nil
	}

	return true, nil
}

// 从文件中load
func (cp *uploadCheckpoint) load(filePath string) error {
	contents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(contents, cp)
	return err
}

// dump到文件
func (cp *uploadCheckpoint) dump(filePath string) error {
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

// 更新分片状态
func (cp *uploadCheckpoint) updatePart(part UploadPart) {
	cp.Parts[part.PartNumber-1].Part = part
	cp.Parts[part.PartNumber-1].IsCompleted = true
}

// 未完成的分片
func (cp *uploadCheckpoint) todoParts() []FileChunk {
	fcs := []FileChunk{}
	for _, part := range cp.Parts {
		if !part.IsCompleted {
			fcs = append(fcs, part.Chunk)
		}
	}
	return fcs
}

// 所有的分片
func (cp *uploadCheckpoint) allParts() []UploadPart {
	ps := []UploadPart{}
	for _, part := range cp.Parts {
		ps = append(ps, part.Part)
	}
	return ps
}

// 完成的字节数
func (cp *uploadCheckpoint) getCompletedBytes() int64 {
	var completedBytes int64
	for _, part := range cp.Parts {
		if part.IsCompleted {
			completedBytes += part.Chunk.Size
		}
	}
	return completedBytes
}

// 计算文件文件MD5
func calcFileMD5(filePath string) (string, error) {
	return "", nil
}

// 初始化分片上传
func prepare(cp *uploadCheckpoint, objectKey, filePath string, partSize int64, bucket *Bucket, options []Option) error {
	// cp
	cp.Magic = uploadCpMagic
	cp.FilePath = filePath
	cp.ObjectKey = objectKey

	// localfile
	fd, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	st, err := fd.Stat()
	if err != nil {
		return err
	}
	cp.FileStat.Size = st.Size()
	cp.FileStat.LastModified = st.ModTime()
	md, err := calcFileMD5(filePath)
	if err != nil {
		return err
	}
	cp.FileStat.MD5 = md

	// chunks
	parts, err := SplitFileByPartSize(filePath, partSize)
	if err != nil {
		return err
	}

	cp.Parts = make([]cpPart, len(parts))
	for i, part := range parts {
		cp.Parts[i].Chunk = part
		cp.Parts[i].IsCompleted = false
	}

	// init load
	imur, err := bucket.InitiateMultipartUpload(objectKey, options...)
	if err != nil {
		return err
	}
	cp.UploadID = imur.UploadID

	return nil
}

// 提交分片上传，删除CP文件
func complete(cp *uploadCheckpoint, bucket *Bucket, parts []UploadPart, cpFilePath string) error {
	imur := InitiateMultipartUploadResult{Bucket: bucket.BucketName,
		Key: cp.ObjectKey, UploadID: cp.UploadID}
	_, err := bucket.CompleteMultipartUpload(imur, parts)
	if err != nil {
		return err
	}
	os.Remove(cpFilePath)
	return err
}

// 并发带断点的上传
func (bucket Bucket) uploadFileWithCp(objectKey, filePath string, partSize int64, options []Option, cpFilePath string, routines int) error {
	listener := getProgressListener(options)

	// LOAD CP数据
	ucp := uploadCheckpoint{}
	err := ucp.load(cpFilePath)
	if err != nil {
		os.Remove(cpFilePath)
	}

	// LOAD出错或数据无效重新初始化上传
	valid, err := ucp.isValid(filePath)
	if err != nil || !valid {
		if err = prepare(&ucp, objectKey, filePath, partSize, &bucket, options); err != nil {
			return err
		}
		os.Remove(cpFilePath)
	}

	chunks := ucp.todoParts()
	imur := InitiateMultipartUploadResult{
		Bucket:   bucket.BucketName,
		Key:      objectKey,
		UploadID: ucp.UploadID}

	jobs := make(chan FileChunk, len(chunks))
	results := make(chan UploadPart, len(chunks))
	failed := make(chan error)
	die := make(chan bool)

	completedBytes := ucp.getCompletedBytes()
	event := newProgressEvent(TransferStartedEvent, completedBytes, ucp.FileStat.Size)
	publishProgress(listener, event)

	// 启动工作协程
	arg := workerArg{&bucket, filePath, imur, uploadPartHooker}
	for w := 1; w <= routines; w++ {
		go worker(w, arg, jobs, results, failed, die)
	}

	// 并发上传分片
	go scheduler(jobs, chunks)

	// 等待分配分片上传完成
	completed := 0
	for completed < len(chunks) {
		select {
		case part := <-results:
			completed++
			ucp.updatePart(part)
			ucp.dump(cpFilePath)
			completedBytes += ucp.Parts[part.PartNumber-1].Chunk.Size
			event = newProgressEvent(TransferDataEvent, completedBytes, ucp.FileStat.Size)
			publishProgress(listener, event)
		case err := <-failed:
			close(die)
			event = newProgressEvent(TransferFailedEvent, completedBytes, ucp.FileStat.Size)
			publishProgress(listener, event)
			return err
		}

		if completed >= len(chunks) {
			break
		}
	}

	event = newProgressEvent(TransferCompletedEvent, completedBytes, ucp.FileStat.Size)
	publishProgress(listener, event)

	// 提交分片上传
	err = complete(&ucp, &bucket, ucp.allParts(), cpFilePath)
	return err
}
