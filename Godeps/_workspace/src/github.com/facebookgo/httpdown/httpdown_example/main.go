package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/facebookgo/httpdown"
)

func handler(w http.ResponseWriter, r *http.Request) {
	duration, err := time.ParseDuration(r.FormValue("duration"))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	fmt.Fprintf(w, "going to sleep %s with pid %d\n", duration, os.Getpid())
	w.(http.Flusher).Flush()
	time.Sleep(duration)
	fmt.Fprintf(w, "slept %s with pid %d\n", duration, os.Getpid())
}

func main() {
	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: http.HandlerFunc(handler),
	}
	hd := &httpdown.HTTP{
		StopTimeout: 10 * time.Second,
		KillTimeout: 1 * time.Second,
	}

	flag.StringVar(&server.Addr, "addr", server.Addr, "http address")
	flag.DurationVar(&hd.StopTimeout, "stop-timeout", hd.StopTimeout, "stop timeout")
	flag.DurationVar(&hd.KillTimeout, "kill-timeout", hd.KillTimeout, "kill timeout")
	flag.Parse()

	if err := httpdown.ListenAndServe(server, hd); err != nil {
		panic(err)
	}
}
