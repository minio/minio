// +build go1.8

package storage

import (
	"io"
	"io/ioutil"
	"net/http"
)

func setContentLengthFromLimitedReader(req *http.Request, lr *io.LimitedReader) {
	req.ContentLength = lr.N
	snapshot := *lr
	req.GetBody = func() (io.ReadCloser, error) {
		r := snapshot
		return ioutil.NopCloser(&r), nil
	}
}
