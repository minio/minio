# XHandler

[![godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/rs/xhandler) [![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/rs/xhandler/master/LICENSE) [![Build Status](https://travis-ci.org/rs/xhandler.svg?branch=master)](https://travis-ci.org/rs/xhandler) [![Coverage](http://gocover.io/_badge/github.com/rs/xhandler)](http://gocover.io/github.com/rs/xhandler)

XHandler is a bridge between [net/context](https://godoc.org/golang.org/x/net/context) and `http.Handler`.

It lets you enforce `net/context` in your handlers without sacrificing compatibility with existing `http.Handlers` nor imposing a specific router.

Thanks to `net/context` deadline management, `xhandler` is able to enforce a per request deadline and will cancel the context when the client closes the connection unexpectedly.

You may create your own `net/context` aware handler pretty much the same way as you would do with http.Handler.

Read more about xhandler on [Dailymotion engineering blog](http://engineering.dailymotion.com/our-way-to-go/).

## Installing

    go get -u github.com/rs/xhandler

## Usage

```go
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/rs/cors"
	"github.com/rs/xhandler"
	"golang.org/x/net/context"
)

type myMiddleware struct {
	next xhandler.HandlerC
}

func (h myMiddleware) ServeHTTPC(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ctx = context.WithValue(ctx, "test", "World")
	h.next.ServeHTTPC(ctx, w, r)
}

func main() {
	c := xhandler.Chain{}

	// Add close notifier handler so context is cancelled when the client closes
	// the connection
	c.UseC(xhandler.CloseHandler)

	// Add timeout handler
	c.UseC(xhandler.TimeoutHandler(2 * time.Second))

	// Middleware putting something in the context
	c.UseC(func(next xhandler.HandlerC) xhandler.HandlerC {
		return myMiddleware{next: next}
	})

	// Mix it with a non-context-aware middleware handler
	c.Use(cors.Default().Handler)

	// Final handler (using handlerFuncC), reading from the context
	xh := xhandler.HandlerFuncC(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		value := ctx.Value("test").(string)
		w.Write([]byte("Hello " + value))
	})

	// Bridge context aware handlers with http.Handler using xhandler.Handle()
	http.Handle("/test", c.Handler(xh))

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
```

### Using xmux

Xhandler comes with an optional context aware [muxer](https://github.com/rs/xmux) forked from [httprouter](https://github.com/julienschmidt/httprouter):

```go
package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/rs/xhandler"
	"github.com/rs/xmux"
	"golang.org/x/net/context"
)

func main() {
	c := xhandler.Chain{}

	// Append a context-aware middleware handler
	c.UseC(xhandler.CloseHandler)

	// Another context-aware middleware handler
	c.UseC(xhandler.TimeoutHandler(2 * time.Second))

	mux := xmux.New()

	// Use c.Handler to terminate the chain with your final handler
	mux.GET("/welcome/:name", xhandler.HandlerFuncC(func(ctx context.Context, w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "Welcome %s!", xmux.Params(ctx).Get("name"))
	}))

	if err := http.ListenAndServe(":8080", c.Handler(mux)); err != nil {
		log.Fatal(err)
	}
}
```

See [xmux](https://github.com/rs/xmux) for more examples.

## Context Aware Middleware

Here is a list of `net/context` aware middleware handlers implementing `xhandler.HandlerC` interface.

Feel free to put up a PR linking your middleware if you have built one:

| Middleware | Author | Description |
| ---------- | ------ | ----------- |
| [xmux](https://github.com/rs/xmux) | [Olivier Poitrey](https://github.com/rs) | HTTP request muxer |
| [xlog](https://github.com/rs/xlog) | [Olivier Poitrey](https://github.com/rs) | HTTP handler logger |
| [xstats](https://github.com/rs/xstats) | [Olivier Poitrey](https://github.com/rs) | A generic client for service instrumentation |
| [xaccess](https://github.com/rs/xaccess) | [Olivier Poitrey](https://github.com/rs) | HTTP handler access logger with [xlog](https://github.com/rs/xlog) and [xstats](https://github.com/rs/xstats) |
| [cors](https://github.com/rs/cors) | [Olivier Poitrey](https://github.com/rs) | [Cross Origin Resource Sharing](http://www.w3.org/TR/cors/) (CORS) support |

## Licenses

All source code is licensed under the [MIT License](https://raw.github.com/rs/xhandler/master/LICENSE).
