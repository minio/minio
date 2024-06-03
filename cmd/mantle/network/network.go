package network

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"

	"github.com/minio/minio/internal/hash"
)

func UploadFormData(client *http.Client, url string, values map[string]io.Reader, headers map[string]string) (putResp PutFileResp, err error) {

	temp, err := os.CreateTemp("", "sds-upload")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer func() {
		temp.Close()
		err = os.Remove(temp.Name())
		if err != nil {
			fmt.Sprintln("cannot delete: %s", temp.Name())
		}

	}()

	w := multipart.NewWriter(temp)

	for key, r := range values {
		var fw io.Writer
		if x, ok := r.(io.Closer); ok {
			defer x.Close()
		}

		if _, ok := r.(*hash.Reader); ok {
			fw, err = w.CreateFormFile(key, "test")
			if err != nil {
				return
			}
		} else {
			fw, err = w.CreateFormField(key)
			if err != nil {
				return
			}
		}
		if _, err = io.Copy(fw, r); err != nil {
			return
		}
	}

	w.Close()
	temp.Seek(0, 0)

	req, err := http.NewRequest(http.MethodPost, url, temp)
	if err != nil {
		return
	}

	setHeaders(headers, req)

	req.Header.Set("Content-type", w.FormDataContentType())

	res, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("ERROR: %s", err)
		return
	}

	if res.StatusCode >= http.StatusBadRequest {
		b, _ := ioutil.ReadAll(res.Body)

		return PutFileResp{}, parseMantleError(b)
	}

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}

	err = json.Unmarshal(bodyBytes, &putResp)
	if err != nil {
		return
	}

	return
}

func Get(client *http.Client, url string, headers map[string]string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	setHeaders(headers, req)

	resp, err = client.Do(req)
	if err != nil {
		//TODO:handle
		return
	}

	if resp.StatusCode != http.StatusOK {
		//TODO:mantle need a fix.
		return nil, errors.New("THIS SHOULD BE FIXED IN MANTLE")
	}

	return
}
