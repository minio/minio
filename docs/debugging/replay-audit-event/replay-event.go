package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/minio/cli"
	"github.com/minio/pkg/v2/workers"
)

type splunkMinIOEvent struct {
	Result struct {
		APIName      string `json:"api.name"`
		APIObject    string `json:"api.object"`
		APIBucket    string `json:"api.bucket"`
		RequestRange string `json:"requestHeader.Range"`
		RequestHost  string `json:"requestHost"`
	} `json:"result"`

	secure bool
}

var defaulTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	MaxIdleConns:          256,
	MaxIdleConnsPerHost:   16,
	ResponseHeaderTimeout: time.Minute,
	IdleConnTimeout:       time.Minute,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 10 * time.Second,
	// Set this value so that the underlying transport round-tripper
	// doesn't try to auto decode the body of objects with
	// content-encoding set to `gzip`.
	//
	// Refer:
	//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
	DisableCompression: true,
}

func replayEvent(event splunkMinIOEvent) {
	switch event.Result.APIName {
	case "GetObject":
	default:
		fmt.Println("skipping the following event, replaying this event is not supported yet", event.Result.APIName)
		return
	}

	scheme := "http"
	if event.secure {
		scheme = "https"
	}

	u, err := url.Parse(scheme + "://" + event.Result.RequestHost + "/" + event.Result.APIBucket + "/" + event.Result.APIObject)
	if err != nil {
		panic(err)
	}

	clnt := &http.Client{
		Transport: defaulTransport,
	}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Range", event.Result.RequestRange)

	resp, err := clnt.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		fmt.Println("event errored with", resp.Status, event.Result.APIName)
	}
}

func main() {
	app := cli.NewApp()
	app.Copyright = "MinIO, Inc."
	app.HideVersion = true
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}

USAGE:
  {{.Name}} {{if .VisibleFlags}}[FLAGS]{{end}}...

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
`

	app.HideHelpCommand = true
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "splunk-file",
		},
		cli.StringFlag{
			Name: "endpoint",
		},
		cli.BoolFlag{
			Name: "secure",
		},
		cli.IntFlag{
			Name:  "concurrent",
			Value: 4,
		},
	}

	app.Action = func(c *cli.Context) error {
		concurrent := c.Int("concurrent")
		requestHost := c.String("endpoint")
		secure := c.Bool("secure")
		wk, err := workers.New(concurrent)
		if err != nil {
			return err
		}

		var events []splunkMinIOEvent

		rd, err := os.Open(c.String("splunk-file"))
		if err != nil {
			return err
		}
		defer rd.Close()

		dec := json.NewDecoder(rd)

		for dec.More() {
			var event splunkMinIOEvent
			if err := dec.Decode(&event); err != nil {
				return err
			}

			if requestHost != "" {
				event.Result.RequestHost = requestHost
			}
			event.secure = secure
			events = append(events, event)
		}

		replayConcurrently := func(n int) {
			for _, event := range events[:n] {
				wk.Take()
				go func(event splunkMinIOEvent) {
					defer wk.Give()

					replayEvent(event)
				}(event)
			}
			wk.Wait()
		}
		for {
			if len(events) < concurrent {
				replayConcurrently(len(events))
				break
			}
			replayConcurrently(concurrent)
			events = events[concurrent:]
		}
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
