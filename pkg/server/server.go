package server

import (
	"log"
	"net/http"
	"reflect"

	"github.com/gorilla/mux"
	"github.com/minio-io/minio/pkg/httpserver"
	storageModule "github.com/minio-io/minio/pkg/storage"
)

var storage *storageModule.Storage

func Start() {
	ctrlChans := make([]chan<- string, 0)
	statusChans := make([]<-chan error, 0)

	ctrlChan, statusChan, storageSystem := storageModule.Start()
	ctrlChans = append(ctrlChans, ctrlChan)
	statusChans = append(statusChans, statusChan)
	storage = storageSystem

	ctrlChan, statusChan = httpserver.Start(getHttpHandler())
	ctrlChans = append(ctrlChans, ctrlChan)
	statusChans = append(statusChans, statusChan)

	cases := createSelectCases(statusChans)

	for len(cases) > 0 {
		chosen, value, recvOk := reflect.Select(cases)
		if recvOk == true {
			// Status Message Received
			log.Println(chosen, value.Interface(), recvOk)
		} else {
			// Channel closed, remove from list
			aliveStatusChans := make([]<-chan error, 0)
			for i, ch := range statusChans {
				if i != chosen {
					aliveStatusChans = append(aliveStatusChans, ch)
				}
			}
			// create new select cases without defunct channel
			statusChans = aliveStatusChans
			cases = createSelectCases(statusChans)
		}
	}
}

func createSelectCases(channels []<-chan error) []reflect.SelectCase {
	cases := make([]reflect.SelectCase, len(channels))
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}
	return cases
}

func getHttpHandler() http.Handler {
	mux := mux.NewRouter()
	mux.HandleFunc("/{bucket}/{object:.*}", getObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", putObjectHandler).Methods("PUT")
	return mux
}

func getObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	storage.CopyObjectToWriter(w, bucket, object)
}

func putObjectHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	bucket := vars["bucket"]
	object := vars["object"]
	storage.StoreObject(bucket, object, req.Body)
}
