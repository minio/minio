// +build !go1.8

package storage

import (
	"io"
	"net/http"
)

func setContentLengthFromLimitedReader(req *http.Request, lr *io.LimitedReader) {
	req.ContentLength = lr.N
}
