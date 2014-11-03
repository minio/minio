package minio

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPrintsGateway(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(GatewayHandler))
	defer server.Close()
	res, err := http.Get(server.URL)
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	bodyString := string(body)
	if bodyString != "Gateway" {
		log.Fatal("Expected 'Gateway', Received '" + bodyString + "'")
	}
}
