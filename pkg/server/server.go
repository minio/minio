package server

import (
	"log"
	"reflect"

	"github.com/minio-io/minio/pkg/httpserver"
	storageModule "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/webapi/minioapi"
)

func Start() {
	ctrlChans := make([]chan<- string, 0)
	statusChans := make([]<-chan error, 0)

	ctrlChan, statusChan, storage := storageModule.Start()
	ctrlChans = append(ctrlChans, ctrlChan)
	statusChans = append(statusChans, statusChan)

	ctrlChan, statusChan = httpserver.Start(minioapi.HttpHandler(storage))
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
