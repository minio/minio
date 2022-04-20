package network

import "net/http"

func setHeaders(headers map[string]string, req *http.Request) {
	for k, v := range headers {
		req.Header.Set(k, v)
	}
}
