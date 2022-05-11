package gateway

import (
	"encoding/json"
	"fmt"
	"github.com/minio/minio/cmd/mantle/network"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

func Put(f *os.File, fn string) (string, error) {
	client := &http.Client{}
	val := map[string]io.Reader{
		"file":        f,
		"displayName": strings.NewReader(fn),
	}

	putResp, err := network.UploadFormData(client, urlJoin("files"), val, setMantleHeaders())
	if err != nil {
		//TODO:handle
		return "", nil
	}

	return putResp.Id, nil
}

func Get(r io.Reader) (bb []byte, err error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}

	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files", string(b)), setMantleHeaders())
	if err != nil {
		return nil, err
	}

	bb, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	return
}

type sdsFileInfo struct {
	Size int64  `json:"size"`
	Id   string `json:"id"`
}

func GetFileSize(id string) (s int64, err error) {
	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files"), setMantleHeaders())
	if err != nil {
		return 0, err
	}

	fis := make([]sdsFileInfo, 0)
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	err = json.Unmarshal(b, &fis)
	if err != nil {
		return
	}

	for _, e := range fis {
		if id == e.Id {
			return e.Size, nil
		}
	}

	fmt.Println("cannot find file in mantle sds")
	return 0, nil
}
