package minioapi

import (
	"net/http"
	"strings"

	"github.com/minio-io/minio/pkg/utils/config"
	"github.com/minio-io/minio/pkg/utils/crypto/signers"
)

type vHandler struct {
	conf    config.Config
	handler http.Handler
}

// grab AccessKey from authorization header
func stripAccessKey(r *http.Request) string {
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) < 2 {
		return ""
	}
	splits := strings.Split(fields[1], ":")
	if len(splits) < 2 {
		return ""
	}
	return splits[0]
}

func validateHandler(conf config.Config, h http.Handler) http.Handler {
	return vHandler{conf, h}
}

func (h vHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	accessKey := stripAccessKey(r)
	if accessKey != "" {
		if err := h.conf.ReadConfig(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			user := h.conf.GetKey(accessKey)
			ok, err := signers.ValidateRequest(user, r)
			if ok {
				h.handler.ServeHTTP(w, r)
			} else {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(err.Error()))
			}
		}
	} else {
		//No access key found, handle this more appropriately
		//TODO: Remove this after adding tests to support signature
		//request
		h.handler.ServeHTTP(w, r)
		//Add this line, to reply back for invalid requests
		//w.WriteHeader(http.StatusUnauthorized)
		//w.Write([]byte("Authorization header malformed")
	}
}

func ignoreUnimplementedResources(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ignoreUnImplementedObjectResources(r) || ignoreUnImplementedBucketResources(r) {
			w.WriteHeader(http.StatusNotImplemented)
		} else {
			h.ServeHTTP(w, r)
		}
	})
}

//// helpers

// Checks requests for unimplemented resources
func ignoreUnImplementedBucketResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if unimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

func ignoreUnImplementedObjectResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if unimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}
