package network

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/minio/minio/internal/hash"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
)

func UploadFormData(client *http.Client, url string, values map[string]io.Reader, headers map[string]string) (putResp PutFileResp, err error) {
	b := bytes.Buffer{}
	w := multipart.NewWriter(&b)

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

	req, err := http.NewRequest(http.MethodPost, url, &b)
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
