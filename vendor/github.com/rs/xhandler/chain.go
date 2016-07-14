package xhandler

import (
	"net/http"

	"golang.org/x/net/context"
)

// Chain is a helper for chaining middleware handlers together for easier
// management.
type Chain []func(next HandlerC) HandlerC

// Add appends a variable number of additional middleware handlers
// to the middleware chain. Middleware handlers can either be
// context-aware or non-context aware handlers with the appropriate
// function signatures.
func (c *Chain) Add(f ...interface{}) {
	for _, h := range f {
		switch v := h.(type) {
		case func(http.Handler) http.Handler:
			c.Use(v)
		case func(HandlerC) HandlerC:
			c.UseC(v)
		default:
			panic("Adding invalid handler to the middleware chain")
		}
	}
}

// With creates a new middleware chain from an existing chain,
// extending it with additional middleware. Middleware handlers
// can either be context-aware or non-context aware handlers
// with the appropriate function signatures.
func (c *Chain) With(f ...interface{}) *Chain {
	n := make(Chain, len(*c))
	copy(n, *c)
	n.Add(f...)
	return &n
}

// UseC appends a context-aware handler to the middleware chain.
func (c *Chain) UseC(f func(next HandlerC) HandlerC) {
	*c = append(*c, f)
}

// Use appends a standard http.Handler to the middleware chain without
// losing track of the context when inserted between two context aware handlers.
//
// Caveat: the f function will be called on each request so you are better off putting
// any initialization sequence outside of this function.
func (c *Chain) Use(f func(next http.Handler) http.Handler) {
	xf := func(next HandlerC) HandlerC {
		return HandlerFuncC(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
			n := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTPC(ctx, w, r)
			})
			f(n).ServeHTTP(w, r)
		})
	}
	*c = append(*c, xf)
}

// Handler wraps the provided final handler with all the middleware appended to
// the chain and returns a new standard http.Handler instance.
// The context.Background() context is injected automatically.
func (c Chain) Handler(xh HandlerC) http.Handler {
	ctx := context.Background()
	return c.HandlerCtx(ctx, xh)
}

// HandlerFC is a helper to provide a function (HandlerFuncC) to Handler().
//
// HandlerFC is equivalent to:
//  c.Handler(xhandler.HandlerFuncC(xhc))
func (c Chain) HandlerFC(xhf HandlerFuncC) http.Handler {
	ctx := context.Background()
	return c.HandlerCtx(ctx, HandlerFuncC(xhf))
}

// HandlerH is a helper to provide a standard http handler (http.HandlerFunc)
// to Handler(). Your final handler won't have access to the context though.
func (c Chain) HandlerH(h http.Handler) http.Handler {
	ctx := context.Background()
	return c.HandlerCtx(ctx, HandlerFuncC(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
	}))
}

// HandlerF is a helper to provide a standard http handler function
// (http.HandlerFunc) to Handler(). Your final handler won't have access
// to the context though.
func (c Chain) HandlerF(hf http.HandlerFunc) http.Handler {
	ctx := context.Background()
	return c.HandlerCtx(ctx, HandlerFuncC(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		hf(w, r)
	}))
}

// HandlerCtx wraps the provided final handler with all the middleware appended to
// the chain and returns a new standard http.Handler instance.
func (c Chain) HandlerCtx(ctx context.Context, xh HandlerC) http.Handler {
	return New(ctx, c.HandlerC(xh))
}

// HandlerC wraps the provided final handler with all the middleware appended to
// the chain and returns a HandlerC instance.
func (c Chain) HandlerC(xh HandlerC) HandlerC {
	for i := len(c) - 1; i >= 0; i-- {
		xh = c[i](xh)
	}
	return xh
}

// HandlerCF wraps the provided final handler func with all the middleware appended to
// the chain and returns a HandlerC instance.
//
// HandlerCF is equivalent to:
//  c.HandlerC(xhandler.HandlerFuncC(xhc))
func (c Chain) HandlerCF(xhc HandlerFuncC) HandlerC {
	return c.HandlerC(HandlerFuncC(xhc))
}
