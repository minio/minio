package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/minio/minio/cmd/mantle/network"
	"github.com/minio/minio/internal/hash"
)

func Put(f *hash.Reader, fn string, configId string) (string, error) {
	client := &http.Client{}
	val := map[string]io.Reader{
		"file":        f,
		"DisplayName": strings.NewReader(fn),
	}

	putResp, err := network.UploadFormData(client, urlJoin("files"), val, setMantleHeaders(configId))
	if err != nil {
		//TODO:handle
		fmt.Println(err.Error())
		return "", err
	}

	return putResp.Id, nil
}

func Get(r io.Reader, configId string) (readCloser io.ReadCloser, bodyLength int64, err error) {
	fileId := GetId(r)

	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files", fileId), setMantleHeaders(configId))
	if err != nil {
		return nil, 0, err
	}

	return resp.Body, resp.ContentLength, nil
}

type sdsFileInfo struct {
	//More fields are available.
	Size            int64  `json:"size"`
	UnencryptedSize int64  `json:"unencryptedSize"`
	Id              string `json:"id"`
}

func GetFileSize(id string) (s int64, err error) {
	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files/info", id), setMantleHeaders(""))
	if err != nil {
		return 0, err
	}

	fi := sdsFileInfo{Id: id}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	err = json.Unmarshal(b, &fi)
	if err != nil {
		return
	}

	if fi.UnencryptedSize > 0 {
		return fi.UnencryptedSize, nil
	}

	fmt.Println("cannot find file in mantle sds")
	return 0, nil
}
