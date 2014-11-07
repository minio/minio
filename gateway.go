package minio

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
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

func GatewayHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Gateway")
}

func GatewayGetObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	fmt.Fprintf(w, "bucket: "+bucket)
	fmt.Fprintf(w, "\r")
	fmt.Fprintf(w, "object: "+object)
}

func RegisterGatewayHandlers(router *mux.Router) {
	router.HandleFunc("/{bucket}/{object:.*}", GatewayGetObjectHandler).Methods("GET")
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
		fmt.Println(objects)
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
