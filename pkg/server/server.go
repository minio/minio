package server

import (
	"log"
	"reflect"

	"github.com/minio-io/minio/pkg/httpserver"
	storageModule "github.com/minio-io/minio/pkg/storage"
	"github.com/minio-io/minio/pkg/webapi/minioapi"
)

func Start() {
	var ctrlChans []chan<- string
	var statusChans []<-chan error

	ctrlChan, statusChan, storage := storageModule.Start()
	ctrlChans = append(ctrlChans, ctrlChan)
	statusChans = append(statusChans, statusChan)

	ctrlChan, statusChan = httpserver.Start(minioapi.HttpHandler(storage), ":8080")
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
			var aliveStatusChans []<-chan error
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
