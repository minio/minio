package zcn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	zerror "github.com/0chain/errors"
	"github.com/0chain/gosdk/zboxcore/fileref"
	"github.com/0chain/gosdk/zboxcore/sdk"
	"github.com/mitchellh/go-homedir"
)

var tempdir string

const (
	PageLimit = 100
	Dir       = "d"
	File      = "f"

	Timeout = time.Second * 30

	DefaultChunkSize = 64 * 1024
	FiveHunderedKB   = 500 * 1024
	OneMB            = 1024 * 1024
	TenMB            = 10 * OneMB
	HundredMB        = 10 * TenMB

	//Error codes
	PathDoesNotExist = "path_no_exist"
)

func init() {
	var err error
	tempdir, err = os.MkdirTemp("", "zcn*")
	if err != nil {
		panic(fmt.Sprintf("could not create tempdir. Error: %v", err))
	}
}

func listRootDir(alloc *sdk.Allocation, fileType string) ([]sdk.ORef, error) {
	var refs []sdk.ORef
	page := 1
	offsetPath := ""

	for {
		//log
		oResult, err := getRegularRefs(alloc, RootPath, offsetPath, fileType, PageLimit)
		if err != nil {
			return nil, err
		}

		refs = append(refs, oResult.Refs...)

		if page >= int(oResult.TotalPages) {
			break
		}

		page++
		offsetPath = oResult.OffsetPath
	}

	return refs, nil
}

func listRegularRefs(alloc *sdk.Allocation, remotePath, marker, fileType string, maxRefs int, isDelimited bool) ([]sdk.ORef, bool, string, []string, error) {
	var refs []sdk.ORef
	var prefixes []string
	var isTruncated bool
	var markedPath string

	remotePath = filepath.Clean(remotePath)
	commonPrefix := getCommonPrefix(remotePath)
	offsetPath := filepath.Join(remotePath, marker)
	for {
		oResult, err := getRegularRefs(alloc, remotePath, offsetPath, fileType, PageLimit)
		if err != nil {
			//log
			return nil, true, "", nil, err
		}
		if len(oResult.Refs) == 0 {
			break
		}

		for i := 0; i < len(oResult.Refs); i++ {
			ref := oResult.Refs[i]
			trimmedPath := strings.TrimPrefix(ref.Path, remotePath+"/")
			if isDelimited {
				if ref.Type == Dir {
					dirPrefix := filepath.Join(commonPrefix, trimmedPath) + "/"
					prefixes = append(prefixes, dirPrefix)
					continue
				}
			}

			ref.Name = filepath.Join(commonPrefix, trimmedPath)

			refs = append(refs, ref)
			if maxRefs != 0 && len(refs) >= maxRefs {
				markedPath = ref.Path
				isTruncated = true
				break
			}
		}

		offsetPath = oResult.OffsetPath

	}
	if isTruncated {
		marker = strings.TrimPrefix(markedPath, remotePath+"/")
	} else {
		marker = ""
	}

	return refs, isTruncated, marker, prefixes, nil
}

func getRegularRefs(alloc *sdk.Allocation, remotePath, offsetPath, fileType string, pageLimit int) (oResult *sdk.ObjectTreeResult, err error) {
	level := len(strings.Split(strings.TrimSuffix(remotePath, "/"), "/")) + 1
	oResult, err = alloc.GetRefs(remotePath, offsetPath, "", "", fileType, "regular", level, pageLimit)
	if err != nil {
		//log error

	} else {
		//log info
	}
	return
}

func getSingleRegularRef(alloc *sdk.Allocation, remotePath string) (*sdk.ORef, error) {
	level := len(strings.Split(strings.TrimSuffix(remotePath, "/"), "/"))
	//log
	oREsult, err := alloc.GetRefs(remotePath, "", "", "", "", "regular", level, 1)
	if err != nil {
		return nil, err
	}

	if len(oREsult.Refs) == 0 {
		return nil, zerror.New(PathDoesNotExist, fmt.Sprintf("remotepath %v does not exist", remotePath))
	}

	return &oREsult.Refs[0], nil
}

func getFileReader(ctx context.Context, alloc *sdk.Allocation, remotePath string) (*os.File, string, error) {
	localFilePath := filepath.Join(tempdir, remotePath)
	os.Remove(localFilePath)

	cb := statusCB{
		doneCh: make(chan struct{}, 1),
		errCh:  make(chan error, 1),
	}

	var ctxCncl context.CancelFunc
	ctx, ctxCncl = context.WithTimeout(ctx, Timeout)
	defer ctxCncl()

	err := alloc.DownloadFile(localFilePath, remotePath, &cb)
	if err != nil {
		return nil, "", err
	}

	select {
	case <-cb.doneCh:
	case err := <-cb.errCh:
		return nil, "", err
	case <-ctx.Done():
		return nil, "", errors.New("exceeded timeout")
	}

	r, err := os.Open(localFilePath)
	if err != nil {
		return nil, "", err
	}

	return r, localFilePath, nil
}

func putFile(ctx context.Context, alloc *sdk.Allocation, remotePath, contentType string, r io.Reader, size int64, isUpdate, shouldEncrypt bool) (err error) {
	cb := &statusCB{
		doneCh: make(chan struct{}, 1),
		errCh:  make(chan error, 1),
	}

	attrs := fileref.Attributes{} //default is owner pays
	_, fileName := filepath.Split(remotePath)
	fileMeta := sdk.FileMeta{
		Path:       "",
		RemotePath: remotePath,
		ActualSize: size,
		MimeType:   contentType,
		RemoteName: fileName,
		Attributes: attrs,
	}

	workDir, err := homedir.Dir()
	if err != nil {
		return err
	}

	var chunkSize int64
	switch {
	case size > HundredMB:
		chunkSize = 2 * TenMB
	case size > TenMB:
		chunkSize = TenMB
	case size > OneMB:
		chunkSize = OneMB
	case size > FiveHunderedKB:
		chunkSize = FiveHunderedKB
	default:
		chunkSize = DefaultChunkSize
	}

	chunkUpload, err := sdk.CreateChunkedUpload(workDir, alloc, fileMeta, newMinioReader(r), isUpdate, false,
		sdk.WithStatusCallback(cb),
		sdk.WithChunkSize(int64(chunkSize)),
	)

	if err != nil {
		return
	}

	err = chunkUpload.Start()
	if err != nil {
		return
	}

	select {
	case <-cb.doneCh:
	case err = <-cb.errCh:
	}

	return
}

func getCommonPrefix(remotePath string) (commonPrefix string) {
	remotePath = strings.TrimSuffix(remotePath, "/")
	pSlice := strings.Split(remotePath, "/")
	if len(pSlice) < 2 {
		return
	}
	/*
		eg: remotePath = "/", return value = ""
		remotePath = "/xyz", return value = ""
		remotePath = "/xyz/abc", return value = "abc"
		remotePath = "/xyz/abc/def", return value = "abc/def"
	*/
	return strings.Join(pSlice[2:], "/")
}

func isPathNoExistError(err error) bool {
	if err == nil {
		return false
	}

	switch err := err.(type) {
	case *zerror.Error:
		if err.Code == PathDoesNotExist {
			return true
		}
	}

	return false
}
