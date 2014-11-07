package minio

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
)

type BucketRequest struct {
	name     string
	context  Context
	callback chan Bucket
}

type Context interface{}

type BucketService interface {
	Serve(chan BucketRequest) Bucket
}

type Bucket interface {
	GetName(Context) string
	Get(Context, string) ([]byte, error)
	Put(Context, string, []byte) error
}

type fakeContext struct{}

func GatewayHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Gateway")
}

type GatewayGetHandler struct {
	requestBucketChan chan BucketRequest
}

func (handler GatewayGetHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucketName := vars["bucket"]
	path := vars["path"]
	context := fakeContext{}
	callback := make(chan Bucket)
	handler.requestBucketChan <- BucketRequest{
		name:     bucketName,
		context:  context,
		callback: callback,
	}
	bucket := <-callback
	object, err := bucket.Get(context, string(path))
	if err != nil {
		http.Error(w, err.Error(), 404)
	} else {
		fmt.Fprintf(w, string(object))
	}
}

type GatewayPutHandler struct {
	requestBucketChan chan BucketRequest
}

func (handler GatewayPutHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucketName := vars["bucket"]
	path := vars["path"]
	object, _ := ioutil.ReadAll(req.Body)
	context := fakeContext{}
	callback := make(chan Bucket)
	handler.requestBucketChan <- BucketRequest{
		name:     bucketName,
		context:  context,
		callback: callback,
	}
	bucket := <-callback
	bucket.Put(context, path, object)
}

func RegisterGatewayHandlers(router *mux.Router) {
	requestBucketChan := make(chan BucketRequest)
	go SynchronizedBucketService(requestBucketChan)
	getHandler := GatewayGetHandler{requestBucketChan: requestBucketChan}
	putHandler := GatewayPutHandler{requestBucketChan: requestBucketChan}
	router.Handle("/{bucket}/{path:.*}", getHandler).Methods("GET")
	router.Handle("/{bucket}/{path:.*}", putHandler).Methods("PUT")
}

func SynchronizedBucketService(input chan BucketRequest) {
	buckets := make(map[string]*SynchronizedBucket)
	for request := range input {
		if buckets[request.name] == nil {
			bucketChannel := make(chan ObjectRequest)
			go inMemoryBucketServer(bucketChannel)
			buckets[request.name] = &SynchronizedBucket{
				name:    request.name,
				channel: bucketChannel,
			}
		}
		request.callback <- buckets[request.name]
	}
	for key := range buckets {
		buckets[key].closeChannel()
	}
}

type SynchronizedBucket struct {
	name    string
	channel chan ObjectRequest
	objects map[string][]byte
}

type ObjectRequest struct {
	requestType string
	path        string
	object      []byte
	callback    chan interface{}
}

func (bucket SynchronizedBucket) GetName(context Context) string {
	return bucket.name
}

func (bucket SynchronizedBucket) Get(context Context, path string) ([]byte, error) {
	callback := make(chan interface{})
	bucket.channel <- ObjectRequest{
		requestType: "GET",
		path:        path,
		callback:    callback,
	}
	switch response := <-callback; response.(type) {
	case []byte:
		return response.([]byte), nil
	case error:
		return nil, response.(error)
	default:
		return nil, errors.New("Unexpected error, service failed")
	}
}

func (bucket SynchronizedBucket) Put(context Context, path string, object []byte) error {
	callback := make(chan interface{})
	bucket.channel <- ObjectRequest{
		requestType: "PUT",
		path:        path,
		object:      object,
		callback:    callback,
	}
	switch response := <-callback; response.(type) {
	case error:
		return response.(error)
	case nil:
		return nil
	default:
		return errors.New("Unexpected error, service failed")
	}
}

func (bucket *SynchronizedBucket) closeChannel() {
	close(bucket.channel)
}

func inMemoryBucketServer(input chan ObjectRequest) {
	objects := make(map[string][]byte)
	for request := range input {
		switch request.requestType {
		case "GET":
			request.callback <- objects[request.path]
		case "PUT":
			objects[request.path] = request.object
			request.callback <- nil
		default:
			request.callback <- errors.New("Unexpected message")
		}
	}
}
