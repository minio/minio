package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio/cmd/mantle/network"
	"github.com/minio/minio/internal/hash"
)

type sdsFile struct {
	ID           string `json:"id"`
	Pointer      string `json:"pointer"`
	FileName     string `json:"fileName"`
	CreationDate string `json:"creationDate"`
}

type sdsFileInfo struct {
	//More fields are available.
	Size            int64  `json:"size"`
	UnencryptedSize int64  `json:"unencryptedSize"`
	Id              string `json:"id"`
}

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

func GetFiles() (files *[]sdsFile, err error) {
	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files"), setMantleHeaders(""))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}

	// Parse the JSON response
	err = json.Unmarshal(body, &files)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return nil, err
	}

	return files, nil
}

func Recovery(root string) {
	fmt.Println("Starting mantle recovery")
	fmt.Println("root is at : ", root)
	files, _ := GetFiles()

	timestamp := time.Now().Format("20060102_150405")
	recoveryDir := filepath.Join(root, timestamp+"_recovery")
	err := os.MkdirAll(recoveryDir, os.ModePerm)
	if err != nil {
		fmt.Println("Error creating recovery directory:", err)
		return
	}

	fileCount := len(*files)

	for idx, file := range *files {
		fullPath := filepath.Join(recoveryDir, file.FileName)
		err := os.MkdirAll(filepath.Dir(fullPath), os.ModePerm)
		if err != nil {
			fmt.Println("Error creating directories:", err)
			continue
		}

		// Write the ID to the file
		err = os.WriteFile(fullPath, []byte(file.ID), 0644)
		if err != nil {
			fmt.Println("Error writing file:", err)
			continue
		}

		//fmt.Printf("Created file %s with content %s\n", fullPath, file.ID)
		fmt.Printf("Done file %d out of %d\n", idx+1, fileCount)
	}
}
